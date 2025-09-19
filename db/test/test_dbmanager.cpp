#include <QTest>
#include <QEventLoop>
#include <QMutex>
#include <QElapsedTimer>
#include <QRandomGenerator>
#include <atomic>
#include "core/DBManager.h"
#include "core/DynamicTable.h"
#include <QTimer>
#include "utils/logger.h"

class DBManagerTest : public QObject {
    Q_OBJECT

private slots:
    void initTestCase() {
        LOG_DEBUG() << "��ʼ���Գ�ʼ��";
        // ʹ���ڲ�������DynamicTable��ʼ��DBManager
        dbManager = new DBManager("test_db.dat", 4);
        QVERIFY(dbManager->initOrLoad());
        QVERIFY(dbManager->isReady());
        LOG_INFO() << "�������ݿ��ʼ���ɹ�";
    }

    void cleanupTestCase() {
        LOG_DEBUG() << "����������ʼ";
        if (dbManager) {
            dbManager->waitForAllTasks(); // ȷ�������������
            delete dbManager;
            dbManager = nullptr;
        }
        
        // ȷ���ļ���ȷɾ�����������ܵ��ļ�����
        const QString dbPath = "test_db.dat";
        int retry = 5;
        while (retry-- > 0 && QFile::exists(dbPath)) {
            if (QFile::remove(dbPath)) {
                LOG_INFO() << "���ݿ��ļ�ɾ���ɹ�:" << dbPath;
                break;
            } else {
                LOG_WARN() << "�ļ�ɾ��ʧ�ܣ�����(" << retry << ")";
                QThread::msleep(100);
            }
        }
        QVERIFY2(!QFile::exists(dbPath), "���ݿ��ļ�ɾ��ʧ��");
    }

    void testSingleReadWrite() {
        LOG_DEBUG() << "���Ե�����¼��д";
        // д���������
        bool writeSuccess = false;
        QEventLoop writeLoop;
        dbManager->submitWriteTask(1, "test_single", 95.5f, 
            [&](bool success, int id) {
                writeSuccess = success;
                writeLoop.quit();
            }
        );
        QTimer::singleShot(5000, &writeLoop, &QEventLoop::quit);
        writeLoop.exec();
        QVERIFY(writeSuccess);

        // ��֤д����
        bool readSuccess = false;
        std::map<QString, DataValue> result;
        QEventLoop readLoop;
        dbManager->submitReadTask(0, 
            [&](bool success, const std::map<QString, DataValue>& data) {
                readSuccess = success;
                if (success) result = data;
                readLoop.quit();
            }
        );
        QTimer::singleShot(5000, &readLoop, &QEventLoop::quit);
        readLoop.exec();
        QVERIFY(readSuccess);
        QCOMPARE(result["id"].intVal, 1);
        QCOMPARE(QString(result["name"].strVal), QString("test_single"));
        QCOMPARE(result["score"].floatVal, 95.5f);
    }

    void testBatchCrypto() {
        LOG_DEBUG() << "��ʼ���ܽ�����������";
    const size_t totalCount = 200; // reduced for faster debug runs
        QElapsedTimer timer;
        timer.start();

        // ����д���������
        LOG_DEBUG() << "д��" << totalCount << "����¼";
        size_t initialCount = dbManager->getRecordCount();
        for (size_t i = 0; i < totalCount; ++i) {
            dbManager->submitWriteTask(i, QString("name_%1").arg(i), 80.0f + (i % 20), nullptr);
        }
        dbManager->waitForAllTasks();
        QCOMPARE(dbManager->getRecordCount(), initialCount + totalCount);
        LOG_INFO() << "д����ɣ���ʱ" << timer.elapsed() << "ms����¼����֤ͨ��";

        // ��������
        QEventLoop encryptLoop;
        std::atomic_size_t encryptSuccess{0};
        QMutex encryptMutex;

        LOG_DEBUG() << "�ύ" << totalCount << "����������";
        timer.restart();
        for (size_t i = 0; i < totalCount; ++i) {
            dbManager->submitCryptoTask(i, DBManager::ENCRYPT,
                [&](bool success, size_t index) {
                    if (success) encryptSuccess++;
                    else LOG_ERROR() << "����ʧ�ܣ�����:" << index;
                    
                    encryptMutex.lock();
                    if (encryptSuccess == totalCount) {
                        encryptLoop.quit();
                    }
                    encryptMutex.unlock();
                }
            );
        }

        // ���ó�ʱ��ÿ1000����¼����1�봦��ʱ�䣩
        QTimer::singleShot(totalCount / 1000 * 1000, &encryptLoop, &QEventLoop::quit);
        encryptLoop.exec();
        QCOMPARE(encryptSuccess, totalCount);
        LOG_INFO() << "������ɣ���ʱ" << timer.elapsed() << "ms";

        // ��֤���ܽ����������10����
        for (int i = 0; i < 10; ++i) {
            size_t randomIndex = static_cast<size_t>(QRandomGenerator::global()->bounded((quint32)totalCount));
            std::map<QString, DataValue> record;
            QVERIFY(dbManager->getTable()->readRecord(randomIndex, record)); // ֱ�ӷ���table��֤
            QString originalName = QString("name_%1").arg(randomIndex);
            QVERIFY(record["name"].strVal != originalName.toUtf8().constData());
        }

        // ��������
        QEventLoop decryptLoop;
        std::atomic_size_t decryptSuccess{0};
        QMutex decryptMutex;

        LOG_DEBUG() << "�ύ" << totalCount << "����������";
        timer.restart();
        for (size_t i = 0; i < totalCount; ++i) {
            dbManager->submitCryptoTask(i, DBManager::DECRYPT,
                [&](bool success, size_t index) {
                    if (success) decryptSuccess++;
                    else LOG_ERROR() << "����ʧ�ܣ�����:" << index;
                    
                    decryptMutex.lock();
                    if (decryptSuccess == totalCount) {
                        decryptLoop.quit();
                    }
                    decryptMutex.unlock();
                }
            );
        }

        QTimer::singleShot(totalCount / 1000 * 1000, &decryptLoop, &QEventLoop::quit);
        decryptLoop.exec();
        QCOMPARE(decryptSuccess, totalCount);
        LOG_INFO() << "������ɣ���ʱ" << timer.elapsed() << "ms";

        // Deterministically verify every record so we can locate the failing index
        for (size_t idx = 0; idx < totalCount; ++idx) {
            std::map<QString, DataValue> record;
            QVERIFY(dbManager->getTable()->readRecord(idx, record));
            QString originalName = QString("name_%1").arg(idx);
            QString readName = QString::fromUtf8(record["name"].strVal, static_cast<int>(record["name"].valueLen));
            if (readName != originalName) {
                LOG_ERROR() << "FINAL_MISMATCH idx=" << idx << " read=" << readName << " expected=" << originalName;
            }
            QCOMPARE(readName, originalName);
        }
    }

    void testBackup() {
        LOG_DEBUG() << "���Ա��ݹ���";
        QString backupPath = "test_backup.zip";
        bool backupSuccess = false;
        QEventLoop backupLoop;

        dbManager->submitBackupTask(backupPath,
            [&](bool success, const QString& path) {
                backupSuccess = success;
                backupLoop.quit();
            }
        );

        QTimer::singleShot(10000, &backupLoop, &QEventLoop::quit);
        backupLoop.exec();
        QVERIFY(backupSuccess);
        QVERIFY(QFile::exists(backupPath));

        // ���������ļ�
        QFile::remove(backupPath);
    }

    void testModifyField() {
        LOG_DEBUG() << "�����ֶ��޸Ĺ���";
        FieldDef newField(TYPE_INT, sizeof(int), "age");
        bool modifySuccess = false;
        QEventLoop modifyLoop;

        dbManager->submitModifyFieldTask(newField,
            [&](bool success, const QString& fieldName) {
                modifySuccess = success;
                modifyLoop.quit();
            }
        );

        QTimer::singleShot(5000, &modifyLoop, &QEventLoop::quit);
        modifyLoop.exec();
        QVERIFY(modifySuccess);

        // ��֤�ֶ�������
        auto& fields = dbManager->getTable()->getHeaderParser().fields;
        bool fieldFound = false;
        for (const auto& field : fields) {
            if (field.name == "age") {
                fieldFound = true;
                break;
            }
        }
        QVERIFY(fieldFound);
    }

private:
    DBManager* dbManager = nullptr;
};

// Use a custom main that creates QCoreApplication so tests can use QEventLoop/QTimer
#include <QCoreApplication>

int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    DBManagerTest tc;
    return QTest::qExec(&tc, argc, argv);
}

#include "test_dbmanager.moc"
