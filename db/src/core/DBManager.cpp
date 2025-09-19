#include "DBManager.h"
#include "utils/ZipUtils.h"
#include "utils/logger.h"
#include <QDir>
#include <QFileInfo>
#include <QDateTime>
#include <QCoreApplication>
#include <QMetaObject>
#include <stdexcept>
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <cstring>
#include <thread>

// AES��Կ��ʵ����ĿӦ�Ӱ�ȫ������ȡ��
const unsigned char DBManager::AES_ENCRYPT_KEY[] = "0123456789abcdef";

// AES implementation using OpenSSL EVP (AES-128-CBC with PKCS#7 padding)
int DBManager::aesEncrypt(const char* input, char* output, size_t len) {
    if (!input || !output || len == 0) return -1;

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        LOG_ERROR() << "EVP_CIPHER_CTX_new failed";
        return -1;
    }

    // Use AES-128-CTR so encrypted output length equals input length (no padding)
    const EVP_CIPHER* cipher = EVP_aes_128_ctr();
    unsigned char iv[AES_BLOCK_SIZE] = {0};

    if (EVP_EncryptInit_ex(ctx, cipher, nullptr, AES_ENCRYPT_KEY, iv) != 1) {
        LOG_ERROR() << "EVP_EncryptInit_ex failed";
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }

    int outLen1 = 0;
    if (EVP_EncryptUpdate(ctx, reinterpret_cast<unsigned char*>(output), &outLen1,
                          reinterpret_cast<const unsigned char*>(input), static_cast<int>(len)) != 1) {
        LOG_ERROR() << "EVP_EncryptUpdate failed";
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }

    int outLen2 = 0;
    if (EVP_EncryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(output) + outLen1, &outLen2) != 1) {
        LOG_ERROR() << "EVP_EncryptFinal_ex failed";
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }

    int total = outLen1 + outLen2;
    LOG_DEBUG() << "AES encrypt produced bytes:" << total;
    EVP_CIPHER_CTX_free(ctx);
    return total;
}

int DBManager::aesDecrypt(const char* input, char* output, size_t len) {
    if (!input || !output || len == 0) return -1;

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        LOG_ERROR() << "EVP_CIPHER_CTX_new failed";
        return -1;
    }

    // Use AES-128-CTR so decrypted output length equals input length (no padding)
    const EVP_CIPHER* cipher = EVP_aes_128_ctr();
    unsigned char iv[AES_BLOCK_SIZE] = {0};

    if (EVP_DecryptInit_ex(ctx, cipher, nullptr, AES_ENCRYPT_KEY, iv) != 1) {
        LOG_ERROR() << "EVP_DecryptInit_ex failed";
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }

    int outLen1 = 0;
    if (EVP_DecryptUpdate(ctx, reinterpret_cast<unsigned char*>(output), &outLen1,
                          reinterpret_cast<const unsigned char*>(input), static_cast<int>(len)) != 1) {
        LOG_ERROR() << "EVP_DecryptUpdate failed";
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }

    int outLen2 = 0;
    if (EVP_DecryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(output) + outLen1, &outLen2) != 1) {
        LOG_ERROR() << "EVP_DecryptFinal_ex failed";
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }

    int total = outLen1 + outLen2;
    LOG_DEBUG() << "AES decrypt produced bytes:" << total;
    EVP_CIPHER_CTX_free(ctx);
    return total;
}

// ������ʵ��
DBManager::ReadTask::ReadTask(DBManager& mgr, size_t index, 
                             std::function<void(bool, const std::map<QString, DataValue>&)> cb)
    : manager(mgr), recordIndex(index), callback(cb) {
    setAutoDelete(true);
    manager.incrementPendingTasks();
}

void DBManager::ReadTask::run() {
    try {
        std::map<QString, DataValue> result;
        bool success = false;
        if (manager.isReady()) {
            // ������ȡ����
            manager.dataMutex.lock();
            success = manager.table->readRecord(recordIndex, result);
            manager.dataMutex.unlock(); // �����ͷ���
        }
        if (callback) {
            // Ensure callbacks are executed in the main thread (queued) to avoid
            // invoking QObject methods (like QEventLoop::quit) from worker threads.
            if (QCoreApplication::instance()) {
                auto cb = callback;
                auto res = result; // copy for queued invocation
                QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, res, success]() mutable {
                    cb(success, res);
                }, Qt::QueuedConnection);
            } else {
                callback(success, result);
            }
        }
        LOG_DEBUG() << "��ȡ��ɣ�����:" << recordIndex << "�ɹ�:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "ReadTask�쳣:" << e.what() << "����:" << recordIndex;
        // ȷ���쳣ʱ�����ͷ�
        if (manager.dataMutex.tryLock()) {
            manager.dataMutex.unlock();
        }
        if (callback) {
            if (QCoreApplication::instance()) {
                auto cb = callback;
                QMetaObject::invokeMethod(QCoreApplication::instance(), [cb]() mutable {
                    cb(false, {});
                }, Qt::QueuedConnection);
            } else {
                callback(false, {});
            }
        }
    }
    manager.decrementPendingTasks();
}

// д����ʵ��
DBManager::WriteTask::WriteTask(DBManager& mgr, int id, const QString& name, float score,
                               std::function<void(bool, int)> cb)
    : manager(mgr), id(id), name(name), score(score), callback(cb) {
    setAutoDelete(true);
    manager.incrementPendingTasks();
}

void DBManager::WriteTask::run() {
    try {
        bool success = false;
        if (manager.isReady()) {
            // ��������������
            std::map<QString, DataValue> data;
            packIntValue(data, "id", id);
            packStringValue(data, "name", name);
            packFloatValue(data, "score", score);
            
            // ����д��ʱ����
            manager.dataMutex.lock();
            success = manager.table->writeRecord(data);
            manager.dataMutex.unlock(); // �����ͷ���
        }
        if (callback) {
            int cbId = id; // copy member to local for safe capture
            if (QCoreApplication::instance()) {
                auto cb = callback;
                QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, success, cbId]() mutable {
                    cb(success, cbId);
                }, Qt::QueuedConnection);
            } else {
                callback(success, cbId);
            }
        }
        LOG_DEBUG() << "д����ɣ�ID:" << id << "�ɹ�:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "WriteTask�쳣:" << e.what() << "ID:" << id;
        // ȷ���쳣ʱ�����ͷ�
        if (manager.dataMutex.tryLock()) {
            manager.dataMutex.unlock();
        }
        if (callback) {
            int cbId = id; // copy member to local for safe capture
            if (QCoreApplication::instance()) {
                auto cb = callback;
                QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, cbId]() mutable {
                    cb(false, cbId);
                }, Qt::QueuedConnection);
            } else {
                callback(false, cbId);
            }
        }
    }
    manager.decrementPendingTasks();
}

// �ӽ�������ʵ��
DBManager::CryptoTask::CryptoTask(DBManager& mgr, size_t index, CryptoType t,
                                 std::function<void(bool, size_t)> cb)
    : manager(mgr), recordIndex(index), type(t), callback(cb) {
    setAutoDelete(true);
    manager.incrementPendingTasks();
}

void DBManager::CryptoTask::run() {
    try {
        bool success = false;
        if (manager.isReady()) {
            std::map<QString, DataValue> record;
            // ������ȡ
            manager.dataMutex.lock();
            bool readSuccess = manager.table->readRecord(recordIndex, record);
            manager.dataMutex.unlock(); // �����ͷŶ���
            
            if (!readSuccess) {
                LOG_WARN() << "��ȡ��¼ʧ�ܣ�����:" << recordIndex;
                if (callback) {
                    if (QCoreApplication::instance()) {
                        auto cb = callback;
                        auto idx = recordIndex;
                        QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, idx]() mutable {
                            cb(false, idx);
                        }, Qt::QueuedConnection);
                    } else {
                        callback(false, recordIndex);
                    }
                }
                manager.decrementPendingTasks();
                return;
            }

            auto it = record.find("name");
            if (it == record.end() || it->second.type != TYPE_STRING) {
                LOG_WARN() << "��¼����Чname�ֶΣ�����:" << recordIndex;
                if (callback) {
                    if (QCoreApplication::instance()) {
                        auto cb = callback;
                        auto idx = recordIndex;
                        QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, idx]() mutable {
                            cb(false, idx);
                        }, Qt::QueuedConnection);
                    } else {
                        callback(false, recordIndex);
                    }
                }
                manager.decrementPendingTasks();
                return;
            }

            // ������мӽ��ܴ���
            const FieldDef* fdef = manager.table->getHeaderParser().fields.size() > 0 ? &manager.table->getHeaderParser().fields[0] : nullptr;
            // Find the corresponding field definition for this DataValue
            const FieldDef* fieldDef = nullptr;
            for (const auto& fd : manager.table->getHeaderParser().fields) {
                if (fd.name == it->first) { fieldDef = &fd; break; }
            }

            size_t targetLen = (fieldDef ? fieldDef->valueLen : it->second.valueLen);

            // Use a temporary buffer to store AES output, but keep the stored
            // DataValue length equal to the field's fixed length. This avoids
            // changing table schema expectations.
            std::vector<char> tmpBuf(targetLen, 0);
            int out = 0;
            if (type == ENCRYPT) {
                // Encrypt using input length = field valueLen, write into tmpBuf
#ifdef ENABLE_RECORD_TRACE
                LOG_DEBUG() << "TRACE CryptoTask before AES ENCRYPT: index=" << recordIndex
                            << " thread=" << std::this_thread::get_id()
                            << " field=" << it->first << " targetLen=" << targetLen;
#endif
                out = manager.aesEncrypt(it->second.strVal, tmpBuf.data(), targetLen);
                LOG_DEBUG() << "CryptoTask::Encrypt produced bytes:" << out << "for record" << recordIndex << "field=" << it->first;
                if (out <= 0) {
                    LOG_ERROR() << "Encryption failed for record" << recordIndex << "field=" << it->first;
                }
            } else {
#ifdef ENABLE_RECORD_TRACE
                LOG_DEBUG() << "TRACE CryptoTask before AES DECRYPT: index=" << recordIndex
                            << " thread=" << std::this_thread::get_id()
                            << " field=" << it->first << " targetLen=" << targetLen;
#endif
                out = manager.aesDecrypt(it->second.strVal, tmpBuf.data(), targetLen);
                LOG_DEBUG() << "CryptoTask::Decrypt produced bytes:" << out << "for record" << recordIndex << "field=" << it->first;
                if (out <= 0) {
                    LOG_ERROR() << "Decryption failed for record" << recordIndex << "field=" << it->first;
                }
            }

            // Only proceed to overwrite and write back if AES produced the expected amount
            if (out == static_cast<int>(targetLen)) {
                // Copy tmpBuf back to DataValue.strVal, but keep valueLen equal to targetLen
                memcpy(it->second.strVal, tmpBuf.data(), targetLen);
                it->second.valueLen = targetLen;

                // ����д�� (overwrite existing record at recordIndex)
#ifdef ENABLE_RECORD_TRACE
                LOG_DEBUG() << "TRACE CryptoTask about to writeRecordAt: index=" << recordIndex
                            << " thread=" << std::this_thread::get_id() << " writeLen=" << targetLen;
#endif
                manager.dataMutex.lock();
                success = manager.table->writeRecordAt(recordIndex, record);
                // read back and verify immediately to detect mismatches early
                if (success) {
                    std::map<QString, DataValue> verifyRec;
                    bool readOk = manager.table->readRecord(recordIndex, verifyRec);
                    if (!readOk) {
                        LOG_ERROR() << "CryptoTask verification read failed for index" << recordIndex;
                    } else {
                        // Use length-aware conversion to avoid relying on NUL termination
                        size_t fieldLen = verifyRec[QString("name")].valueLen;
                        QString written = QString::fromUtf8(verifyRec[QString("name")].strVal, static_cast<int>(fieldLen));
                        size_t expectedLen = record[QString("name")].valueLen;
                        QString expected = QString::fromUtf8(record[QString("name")].strVal, static_cast<int>(expectedLen));
                        if (written != expected) {
                            LOG_ERROR() << "CryptoTask verification mismatch at" << recordIndex << "written:" << written << "expected:" << expected;
#ifdef ENABLE_RECORD_TRACE
                            LOG_DEBUG() << "TRACE CryptoTask verification FAIL index=" << recordIndex
                                        << " thread=" << std::this_thread::get_id()
                                        << " written_raw='" << QString::fromUtf8(verifyRec[QString("name")].strVal, static_cast<int>(verifyRec[QString("name")].valueLen))
                                        << "' expected_raw='" << QString::fromUtf8(record[QString("name")].strVal, static_cast<int>(record[QString("name")].valueLen)) << "'";
#endif
                        }
                    }
                }
                manager.dataMutex.unlock(); // �����ͷ�д��
            } else {
                LOG_ERROR() << "CryptoTask: AES produced unexpected byte count (" << out << ") for record" << recordIndex << "field=" << it->first << ", skipping write";
                success = false;
            }
        } else {
            LOG_ERROR() << "DBManagerδ��ʼ�����޷�ִ�мӽ���";
        }
        if (callback) {
            if (QCoreApplication::instance()) {
                auto cb = callback;
                auto idx = recordIndex;
                QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, idx, success]() mutable {
                    cb(success, idx);
                }, Qt::QueuedConnection);
            } else {
                callback(success, recordIndex);
            }
        }
        LOG_DEBUG() << (type == ENCRYPT ? "����" : "����") << "��ɣ�����:" << recordIndex << "�ɹ�:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "CryptoTask�쳣:" << e.what() << "����:" << recordIndex;
        // ȷ���쳣ʱ�����ͷ�
        if (manager.dataMutex.tryLock()) {
            manager.dataMutex.unlock();
        }
        if (callback) callback(false, recordIndex);
    }
    manager.decrementPendingTasks();
}

// ��������ʵ��
DBManager::BackupTask::BackupTask(DBManager& mgr, const QString& path,
                                 std::function<void(bool, const QString&)> cb)
    : manager(mgr), backupPath(path), callback(cb) {
    setAutoDelete(true);
    manager.incrementPendingTasks();
}

void DBManager::BackupTask::run() {
    bool success = false;
    QString resultPath;
    try {
        QFileInfo pathInfo(backupPath);
        QDir parentDir = pathInfo.dir();
        if (!parentDir.exists() && !parentDir.mkpath(".")) {
            throw std::runtime_error("�޷���������Ŀ¼");
        }

        QFile sourceFile(manager.getDbPath());
        if (!sourceFile.exists() || !sourceFile.open(QIODevice::ReadOnly)) {
            throw std::runtime_error("Դ���ݿ��ļ��޷���ȡ");
        }
        sourceFile.close();

        // �������ݣ���ֹ����ʱ���ṹ�����
        manager.metaMutex.lock();
        success = ZipUtils::compressFile(manager.getDbPath(), backupPath);
        manager.metaMutex.unlock(); // �����ͷ���
        resultPath = backupPath;
        LOG_INFO() << "������ɣ�·��:" << resultPath << "�ɹ�:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "���������쳣:" << e.what();
        // ȷ���쳣ʱ�����ͷ�
        if (manager.metaMutex.tryLock()) {
            manager.metaMutex.unlock();
        }
    }
    if (callback) {
        if (QCoreApplication::instance()) {
            auto cb = callback;
            auto path = resultPath;
            QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, path, success]() mutable {
                cb(success, path);
            }, Qt::QueuedConnection);
        } else {
            callback(success, resultPath);
        }
    }
    manager.decrementPendingTasks();
}

// �޸��ֶ�����ʵ��
DBManager::ModifyFieldTask::ModifyFieldTask(DBManager& mgr, const FieldDef& field,
                                           std::function<void(bool, const QString&)> cb)
    : manager(mgr), newField(field), callback(cb) {
    setAutoDelete(true);
    manager.incrementPendingTasks();
}

void DBManager::ModifyFieldTask::run() {
    try {
        bool success = false;
        if (manager.isReady()) {
            // �����޸�Ԫ����
            manager.metaMutex.lock();
            success = manager.table->addField(newField);
            manager.metaMutex.unlock(); // �����ͷ���
        }
        if (callback) {
            if (QCoreApplication::instance()) {
                auto cb = callback;
                auto name = newField.name;
                QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, name, success]() mutable {
                    cb(success, name);
                }, Qt::QueuedConnection);
            } else {
                callback(success, newField.name);
            }
        }
        LOG_DEBUG() << "�޸��ֶ���ɣ��ֶ�:" << newField.name << "�ɹ�:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "ModifyFieldTask�쳣:" << e.what() << "�ֶ�:" << newField.name;
        // ȷ���쳣ʱ�����ͷ�
        if (manager.metaMutex.tryLock()) {
            manager.metaMutex.unlock();
        }
        if (callback) {
            if (QCoreApplication::instance()) {
                auto cb = callback;
                auto name = newField.name;
                QMetaObject::invokeMethod(QCoreApplication::instance(), [cb, name]() mutable {
                    cb(false, name);
                }, Qt::QueuedConnection);
            } else {
                callback(false, newField.name);
            }
        }
    }
    manager.decrementPendingTasks();
}

// ���ݴ�����ߺ���
void DBManager::packIntValue(std::map<QString, DataValue>& data, const QString& key, int value) {
    DataValue val(TYPE_INT, sizeof(int));
    val.intVal = value;
    data[key] = val;
}

void DBManager::packStringValue(std::map<QString, DataValue>& data, const QString& key, const QString& value) {
    DataValue val(TYPE_STRING, FIXED_STRING_LENGTH);
    QByteArray bytes = value.toUtf8();
    memcpy(val.strVal, bytes.constData(), std::min((size_t)bytes.size(), val.valueLen));
    data[key] = val;
}

void DBManager::packFloatValue(std::map<QString, DataValue>& data, const QString& key, float value) {
    DataValue val(TYPE_FLOAT, sizeof(float));
    val.floatVal = value;
    data[key] = val;
}

// �����������
void DBManager::incrementPendingTasks() {
    taskMutex.lock();
    pendingTasks++;
    taskMutex.unlock();
}

void DBManager::decrementPendingTasks() {
    taskMutex.lock();
    if (--pendingTasks == 0) {
        taskCondition.wakeAll();
    }
    taskMutex.unlock();
}

// �ⲿDynamicTable���캯��
DBManager::DBManager(DynamicTable* externalTable, int maxThreadCount) 
    : dbPath("external_table"), table(externalTable), ownTable(false) {  // 递归锁由默认构造
    if (!externalTable) {
        throw std::invalid_argument("External DynamicTable cannot be null");
    }

    // �̳߳س�ʼ��
    int optimalThreads = std::max(1, std::min(maxThreadCount, QThread::idealThreadCount()));
    threadPool.setMaxThreadCount(optimalThreads);
    threadPool.setExpiryTimeout(-1); // ������ʱ
    LOG_INFO() << "�̳߳س�ʼ����ɣ��߳���:" << optimalThreads;

    // ���ⲿ����ȡ�ֶ���Ϣ
    baseFields = externalTable->getHeaderParser().fields;
    LOG_INFO() << "ʹ���ⲿDynamicTable��ʼ��DBManager���ֶ�����:" << baseFields.size();

    isInitialized = true;
}

// �ڲ�DynamicTable���캯��
DBManager::DBManager(const QString& path, int maxThreadCount) 
    : dbPath(path), table(new DynamicTable()), ownTable(true) {  // 递归锁由默认构造
    // �̳߳س�ʼ��
    int optimalThreads = std::max(1, std::min(maxThreadCount, QThread::idealThreadCount()));
    threadPool.setMaxThreadCount(optimalThreads);
    threadPool.setExpiryTimeout(-1); // ������ʱ
    LOG_INFO() << "�̳߳س�ʼ����ɣ��߳���:" << optimalThreads;
    
    // ��ʼ�������ֶ�
    FieldDef idField{TYPE_INT, sizeof(int), "id"};
    FieldDef nameField{TYPE_STRING, FIXED_STRING_LENGTH, "name"};
    FieldDef scoreField{TYPE_FLOAT, sizeof(float), "score"};
    baseFields = {idField, nameField, scoreField};
}

// ��������
DBManager::~DBManager() {
    waitForAllTasks();
    threadPool.clear();
    threadPool.waitForDone();
    
    // �ͷ��ڲ�������DynamicTable
    if (ownTable && table) {
        delete table;
        table = nullptr;
    }
    LOG_INFO() << "DBManager������";
}

// ��ʼ����������ݿ�
bool DBManager::initOrLoad() {
    metaMutex.lock(); // Ԫ������������ʼ������
    if (isInitialized) {
        metaMutex.unlock();
        return true;
    }

    // �ⲿ�������ʼ����ֱ�Ӽ��״̬
    if (!ownTable) {
        isInitialized = table->isTableLoaded();
        metaMutex.unlock();
        return isInitialized;
    }

    // �ڲ�����ʼ��/����
    QFileInfo fileInfo(dbPath);
    bool success = false;

    if (fileInfo.exists()) {
        success = table->loadTable(dbPath);
        LOG_INFO() << "���Լ������ݿ�:" << dbPath << "���:" << success;
    } else {
        success = table->initTable(dbPath, baseFields);
        LOG_INFO() << "���Դ������ݿ�:" << dbPath << "���:" << success;
    }

    if (success) {
        isInitialized = true;
    } else {
        LOG_ERROR() << "���ݿ��ʼ��ʧ�ܣ�·��:" << dbPath;
    }

    metaMutex.unlock();
    return success;
}

// �����ύ�ӿ�
void DBManager::submitReadTask(size_t index, std::function<void(bool, const std::map<QString, DataValue>&)> callback) {
    threadPool.start(new ReadTask(*this, index, callback));
}

void DBManager::submitWriteTask(int id, const QString& name, float score, std::function<void(bool, int)> callback) {
    threadPool.start(new WriteTask(*this, id, name, score, callback));
}

void DBManager::submitCryptoTask(size_t index, CryptoType type, std::function<void(bool, size_t)> callback) {
    threadPool.start(new CryptoTask(*this, index, type, callback));
}

void DBManager::submitBackupTask(const QString& path, std::function<void(bool, const QString&)> callback) {
    threadPool.start(new BackupTask(*this, path, callback));
}

void DBManager::submitModifyFieldTask(const FieldDef& field, std::function<void(bool, const QString&)> callback) {
    threadPool.start(new ModifyFieldTask(*this, field, callback));
}

// �ȴ������������
void DBManager::waitForAllTasks() {
    taskMutex.lock();
    while (pendingTasks > 0) {
        LOG_DEBUG() << "�ȴ�������ɣ�ʣ��:" << pendingTasks;
        taskCondition.wait(&taskMutex);
    }
    taskMutex.unlock();
}

// ����ÿ�ձ���
void DBManager::enableDailyBackup(const QString& backupDir, int keepDays) {
    LOG_INFO() << "����ÿ�ձ��ݣ�Ŀ¼:" << backupDir << "��������:" << keepDays;
    // ʵ����Ŀ��ʵ�ֶ�ʱ�����߼�
}
