#include <QTest>
#include <QEventLoop>
#include <QMutex>
#include <QElapsedTimer>
#include <atomic>
#include "core/DBManager.h"
#include "core/DynamicTable.h"
#include <QTimer>
#include "utils/logger.h"

class DBManagerTest : public QObject {
    Q_OBJECT

private slots:
    void initTestCase() {
        LOG_DEBUG() << "开始测试初始化";
        // 使用内部创建的DynamicTable初始化DBManager
        dbManager = new DBManager("test_db.dat", 4);
        QVERIFY(dbManager->initOrLoad());
        QVERIFY(dbManager->isReady());
        LOG_INFO() << "测试数据库初始化成功";
    }

    void cleanupTestCase() {
        LOG_DEBUG() << "测试清理开始";
        if (dbManager) {
            dbManager->waitForAllTasks(); // 确保所有任务完成
            delete dbManager;
            dbManager = nullptr;
        }
        
        // 确保文件正确删除（处理可能的文件锁）
        const QString dbPath = "test_db.dat";
        int retry = 5;
        while (retry-- > 0 && QFile::exists(dbPath)) {
            if (QFile::remove(dbPath)) {
                LOG_INFO() << "数据库文件删除成功:" << dbPath;
                break;
            } else {
                LOG_WARN() << "文件删除失败，重试(" << retry << ")";
                QThread::msleep(100);
            }
        }
        QVERIFY2(!QFile::exists(dbPath), "数据库文件删除失败");
    }

    void testSingleReadWrite() {
        LOG_DEBUG() << "测试单条记录读写";
        // 写入测试数据
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

        // 验证写入结果
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
        LOG_DEBUG() << "开始加密解密批量测试";
        const size_t totalCount = 10000;
        QElapsedTimer timer;
        timer.start();

        // 批量写入测试数据
        LOG_DEBUG() << "写入" << totalCount << "条记录";
        for (size_t i = 0; i < totalCount; ++i) {
            dbManager->submitWriteTask(i, QString("name_%1").arg(i), 80.0f + (i % 20), nullptr);
        }
        dbManager->waitForAllTasks();
        QCOMPARE(dbManager->getRecordCount(), totalCount);
        LOG_INFO() << "写入完成，耗时" << timer.elapsed() << "ms，记录数验证通过";

        // 批量加密
        QEventLoop encryptLoop;
        std::atomic_size_t encryptSuccess{0};
        QMutex encryptMutex;

        LOG_DEBUG() << "提交" << totalCount << "条加密任务";
        timer.restart();
        for (size_t i = 0; i < totalCount; ++i) {
            dbManager->submitCryptoTask(i, DBManager::ENCRYPT,
                [&](bool success, size_t index) {
                    if (success) encryptSuccess++;
                    else LOG_ERROR() << "加密失败，索引:" << index;
                    
                    encryptMutex.lock();
                    if (encryptSuccess == totalCount) {
                        encryptLoop.quit();
                    }
                    encryptMutex.unlock();
                }
            );
        }

        // 设置超时（每1000条记录允许1秒处理时间）
        QTimer::singleShot(totalCount / 1000 * 1000, &encryptLoop, &QEventLoop::quit);
        encryptLoop.exec();
        QCOMPARE(encryptSuccess, totalCount);
        LOG_INFO() << "加密完成，耗时" << timer.elapsed() << "ms";

        // 验证加密结果（随机抽查10条）
        for (int i = 0; i < 10; ++i) {
            size_t randomIndex = qrand() % totalCount;
            std::map<QString, DataValue> record;
            QVERIFY(dbManager->getTable()->readRecord(randomIndex, record)); // 直接访问table验证
            QString originalName = QString("name_%1").arg(randomIndex);
            QVERIFY(record["name"].strVal != originalName.toUtf8().constData());
        }

        // 批量解密
        QEventLoop decryptLoop;
        std::atomic_size_t decryptSuccess{0};
        QMutex decryptMutex;

        LOG_DEBUG() << "提交" << totalCount << "条解密任务";
        timer.restart();
        for (size_t i = 0; i < totalCount; ++i) {
            dbManager->submitCryptoTask(i, DBManager::DECRYPT,
                [&](bool success, size_t index) {
                    if (success) decryptSuccess++;
                    else LOG_ERROR() << "解密失败，索引:" << index;
                    
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
        LOG_INFO() << "解密完成，耗时" << timer.elapsed() << "ms";

        // 验证解密结果（随机抽查10条）
        for (int i = 0; i < 10; ++i) {
            size_t randomIndex = qrand() % totalCount;
            std::map<QString, DataValue> record;
            QVERIFY(dbManager->getTable()->readRecord(randomIndex, record));
            QString originalName = QString("name_%1").arg(randomIndex);
            QCOMPARE(QString(record["name"].strVal), originalName);
        }
    }

    void testBackup() {
        LOG_DEBUG() << "测试备份功能";
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

        // 清理备份文件
        QFile::remove(backupPath);
    }

    void testModifyField() {
        LOG_DEBUG() << "测试字段修改功能";
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

        // 验证字段已添加
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

QTEST_APPLESS_MAIN(DBManagerTest)
#include "test_dbmanager.moc"
