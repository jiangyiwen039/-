#include "DBManager.h"
#include "utils/ZipUtils.h"
#include "utils/logger.h"
#include <QDir>
#include <QFileInfo>
#include <QDateTime>
#include <stdexcept>
#include <openssl/aes.h>
#include <cstring>
#include <openssl/evp.h>  

// AES��Կ��ʵ����ĿӦ�Ӱ�ȫ������ȡ��
const unsigned char DBManager::AES_ENCRYPT_KEY[] = "0123456789abcdef";

// AES����ʵ��
void DBManager::aesEncrypt(const char* input, char* output, size_t len) {
    if (!input || !output || len == 0) return;
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        LOG_ERROR() << "EVP�����Ĵ���ʧ��";
        return;
    }
    // ��ʼ�����������ģ�AES-128-CBC��
    if (EVP_EncryptInit_ex(ctx, EVP_aes_128_cbc(), nullptr, AES_ENCRYPT_KEY, nullptr) != 1) {
        LOG_ERROR() << "AES���ܳ�ʼ��ʧ��";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }

    int out_len1 = 0, out_len2 = 0;
    // ִ�м���
    if (EVP_EncryptUpdate(ctx, reinterpret_cast<unsigned char*>(output), &out_len1,
                         reinterpret_cast<const unsigned char*>(input), len) != 1) {
        LOG_ERROR() << "AES���ܹ���ʧ��";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }
    // �������ܣ�����ʣ�����ݣ�
    if (EVP_EncryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(output) + out_len1, &out_len2) != 1) {
        LOG_ERROR() << "AES���ܽ���ʧ��";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }

    EVP_CIPHER_CTX_free(ctx);
    LOG_DEBUG() << "AES������ɣ�����:" << len;
}

// AES����ʵ��
void DBManager::aesDecrypt(const char* input, char* output, size_t len) {
    if (!input || !output || len == 0) return;

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        LOG_ERROR() << "EVP�����Ĵ���ʧ��";
        return;
    }

    // ��ʼ������������
    if (EVP_DecryptInit_ex(ctx, EVP_aes_128_cbc(), nullptr, AES_ENCRYPT_KEY, nullptr) != 1) {
        LOG_ERROR() << "AES���ܳ�ʼ��ʧ��";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }

    int out_len1 = 0, out_len2 = 0;
    // ִ�н���
    if (EVP_DecryptUpdate(ctx, reinterpret_cast<unsigned char*>(output), &out_len1,
                         reinterpret_cast<const unsigned char*>(input), len) != 1) {
        LOG_ERROR() << "AES���ܹ���ʧ��";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }
    // ��������
    if (EVP_DecryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(output) + out_len1, &out_len2) != 1) {
        LOG_ERROR() << "AES���ܽ���ʧ��";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }

    EVP_CIPHER_CTX_free(ctx);
    LOG_DEBUG() << "AES������ɣ�����:" << len;
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
        if (callback) callback(success, result);
        LOG_DEBUG() << "��ȡ��ɣ�����:" << recordIndex << "�ɹ�:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "ReadTask�쳣:" << e.what() << "����:" << recordIndex;
        // ȷ���쳣ʱ�����ͷ�
        if (manager.dataMutex.tryLock()) {
            manager.dataMutex.unlock();
        }
        if (callback) callback(false, {});
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
        if (callback) callback(success, id);
        LOG_DEBUG() << "д����ɣ�ID:" << id << "�ɹ�:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "WriteTask�쳣:" << e.what() << "ID:" << id;
        // ȷ���쳣ʱ�����ͷ�
        if (manager.dataMutex.tryLock()) {
            manager.dataMutex.unlock();
        }
        if (callback) callback(false, id);
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
                callback(false, recordIndex);
                manager.decrementPendingTasks();
                return;
            }

            auto it = record.find("name");
            if (it == record.end() || it->second.type != TYPE_STRING) {
                LOG_WARN() << "��¼����Чname�ֶΣ�����:" << recordIndex;
                callback(false, recordIndex);
                manager.decrementPendingTasks();
                return;
            }

            // ������мӽ��ܴ���
            if (type == ENCRYPT) {
                manager.aesEncrypt(it->second.strVal, it->second.strVal, it->second.valueLen);
            } else {
                manager.aesDecrypt(it->second.strVal, it->second.strVal, it->second.valueLen);
            }

            // ����д��
            manager.dataMutex.lock();
            success = manager.table->writeRecord(record);
            manager.dataMutex.unlock(); // �����ͷ�д��
        } else {
            LOG_ERROR() << "DBManagerδ��ʼ�����޷�ִ�мӽ���";
        }
        if (callback) callback(success, recordIndex);
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

        // �������ݣ���ֹ����ʱ��ṹ�����
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
    if (callback) callback(success, resultPath);
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
        if (callback) callback(success, newField.name);
        LOG_DEBUG() << "�޸��ֶ���ɣ��ֶ�:" << newField.name << "�ɹ�:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "ModifyFieldTask�쳣:" << e.what() << "�ֶ�:" << newField.name;
        // ȷ���쳣ʱ�����ͷ�
        if (manager.metaMutex.tryLock()) {
            manager.metaMutex.unlock();
        }
        if (callback) callback(false, newField.name);
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
    : dbPath("external_table"), table(externalTable), ownTable(false),
      metaMutex(QMutex::Recursive), dataMutex(QMutex::Recursive) {  // �ݹ���֧������
    if (!externalTable) {
        throw std::invalid_argument("External DynamicTable cannot be null");
    }

    // �̳߳س�ʼ��
    int optimalThreads = std::max(1, std::min(maxThreadCount, QThread::idealThreadCount()));
    threadPool.setMaxThreadCount(optimalThreads);
    threadPool.setExpiryTimeout(-1); // ������ʱ
    LOG_INFO() << "�̳߳س�ʼ����ɣ��߳���:" << optimalThreads;

    // ���ⲿ���ȡ�ֶ���Ϣ
    baseFields = externalTable->getHeaderParser().fields;
    LOG_INFO() << "ʹ���ⲿDynamicTable��ʼ��DBManager���ֶ�����:" << baseFields.size();

    isInitialized = true;
}

// �ڲ�DynamicTable���캯��
DBManager::DBManager(const QString& path, int maxThreadCount) 
    : dbPath(path), table(new DynamicTable()), ownTable(true),
      metaMutex(QMutex::Recursive), dataMutex(QMutex::Recursive) {  // �ݹ���֧������
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

    // �ڲ����ʼ��/����
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

// ʵ��"·��+�߳���"���캯��
DBManager::DBManager(const QString& path, int maxThreadCount) 
    : dbPath(path), table(nullptr), ownTable(true), threadPool(maxThreadCount), pendingTasks(0) {
    // ��ʼ�������ֶΣ�����ҵ�������壬����id��name��score��
    baseFields = {
        FieldDef(TYPE_INT, sizeof(int), "id"),
        FieldDef(TYPE_STRING, 64, "name"),  // �����ַ�������64
        FieldDef(TYPE_FLOAT, sizeof(float), "score")
    };
    threadPool.setMaxThreadCount(maxThreadCount);
}

// ʵ��"�ⲿDynamicTable+�߳���"���캯��
DBManager::DBManager(DynamicTable* externalTable, int maxThreadCount)
    : table(externalTable), ownTable(false), threadPool(maxThreadCount), pendingTasks(0) {
    threadPool.setMaxThreadCount(maxThreadCount);
    if (table) {
        // ���ⲿ���ȡ�ֶ���Ϣ
        baseFields = table->getHeaderParser().fields;
    }
}

// ��������ʵ��
DBManager::~DBManager() {
    waitForAllTasks();
    if (ownTable && table) {
        delete table;  // ����ӵ������Ȩʱ����
    }
}
