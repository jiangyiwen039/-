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

// AES密钥（实际项目应从安全渠道获取）
const unsigned char DBManager::AES_ENCRYPT_KEY[] = "0123456789abcdef";

// AES加密实现
void DBManager::aesEncrypt(const char* input, char* output, size_t len) {
    if (!input || !output || len == 0) return;
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        LOG_ERROR() << "EVP上下文创建失败";
        return;
    }
    // 初始化加密上下文（AES-128-CBC）
    if (EVP_EncryptInit_ex(ctx, EVP_aes_128_cbc(), nullptr, AES_ENCRYPT_KEY, nullptr) != 1) {
        LOG_ERROR() << "AES加密初始化失败";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }

    int out_len1 = 0, out_len2 = 0;
    // 执行加密
    if (EVP_EncryptUpdate(ctx, reinterpret_cast<unsigned char*>(output), &out_len1,
                         reinterpret_cast<const unsigned char*>(input), len) != 1) {
        LOG_ERROR() << "AES加密过程失败";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }
    // 结束加密（处理剩余数据）
    if (EVP_EncryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(output) + out_len1, &out_len2) != 1) {
        LOG_ERROR() << "AES加密结束失败";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }

    EVP_CIPHER_CTX_free(ctx);
    LOG_DEBUG() << "AES加密完成，长度:" << len;
}

// AES解密实现
void DBManager::aesDecrypt(const char* input, char* output, size_t len) {
    if (!input || !output || len == 0) return;

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        LOG_ERROR() << "EVP上下文创建失败";
        return;
    }

    // 初始化解密上下文
    if (EVP_DecryptInit_ex(ctx, EVP_aes_128_cbc(), nullptr, AES_ENCRYPT_KEY, nullptr) != 1) {
        LOG_ERROR() << "AES解密初始化失败";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }

    int out_len1 = 0, out_len2 = 0;
    // 执行解密
    if (EVP_DecryptUpdate(ctx, reinterpret_cast<unsigned char*>(output), &out_len1,
                         reinterpret_cast<const unsigned char*>(input), len) != 1) {
        LOG_ERROR() << "AES解密过程失败";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }
    // 结束解密
    if (EVP_DecryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(output) + out_len1, &out_len2) != 1) {
        LOG_ERROR() << "AES解密结束失败";
        EVP_CIPHER_CTX_free(ctx);
        return;
    }

    EVP_CIPHER_CTX_free(ctx);
    LOG_DEBUG() << "AES解密完成，长度:" << len;
}
// 读任务实现
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
            // 加锁读取数据
            manager.dataMutex.lock();
            success = manager.table->readRecord(recordIndex, result);
            manager.dataMutex.unlock(); // 立即释放锁
        }
        if (callback) callback(success, result);
        LOG_DEBUG() << "读取完成，索引:" << recordIndex << "成功:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "ReadTask异常:" << e.what() << "索引:" << recordIndex;
        // 确保异常时锁已释放
        if (manager.dataMutex.tryLock()) {
            manager.dataMutex.unlock();
        }
        if (callback) callback(false, {});
    }
    manager.decrementPendingTasks();
}

// 写任务实现
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
            // 先在锁外打包数据
            std::map<QString, DataValue> data;
            packIntValue(data, "id", id);
            packStringValue(data, "name", name);
            packFloatValue(data, "score", score);
            
            // 仅在写入时加锁
            manager.dataMutex.lock();
            success = manager.table->writeRecord(data);
            manager.dataMutex.unlock(); // 立即释放锁
        }
        if (callback) callback(success, id);
        LOG_DEBUG() << "写入完成，ID:" << id << "成功:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "WriteTask异常:" << e.what() << "ID:" << id;
        // 确保异常时锁已释放
        if (manager.dataMutex.tryLock()) {
            manager.dataMutex.unlock();
        }
        if (callback) callback(false, id);
    }
    manager.decrementPendingTasks();
}

// 加解密任务实现
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
            // 加锁读取
            manager.dataMutex.lock();
            bool readSuccess = manager.table->readRecord(recordIndex, record);
            manager.dataMutex.unlock(); // 立即释放读锁
            
            if (!readSuccess) {
                LOG_WARN() << "读取记录失败，索引:" << recordIndex;
                callback(false, recordIndex);
                manager.decrementPendingTasks();
                return;
            }

            auto it = record.find("name");
            if (it == record.end() || it->second.type != TYPE_STRING) {
                LOG_WARN() << "记录无有效name字段，索引:" << recordIndex;
                callback(false, recordIndex);
                manager.decrementPendingTasks();
                return;
            }

            // 锁外进行加解密处理
            if (type == ENCRYPT) {
                manager.aesEncrypt(it->second.strVal, it->second.strVal, it->second.valueLen);
            } else {
                manager.aesDecrypt(it->second.strVal, it->second.strVal, it->second.valueLen);
            }

            // 加锁写入
            manager.dataMutex.lock();
            success = manager.table->writeRecord(record);
            manager.dataMutex.unlock(); // 立即释放写锁
        } else {
            LOG_ERROR() << "DBManager未初始化，无法执行加解密";
        }
        if (callback) callback(success, recordIndex);
        LOG_DEBUG() << (type == ENCRYPT ? "加密" : "解密") << "完成，索引:" << recordIndex << "成功:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "CryptoTask异常:" << e.what() << "索引:" << recordIndex;
        // 确保异常时锁已释放
        if (manager.dataMutex.tryLock()) {
            manager.dataMutex.unlock();
        }
        if (callback) callback(false, recordIndex);
    }
    manager.decrementPendingTasks();
}

// 备份任务实现
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
            throw std::runtime_error("无法创建备份目录");
        }

        QFile sourceFile(manager.getDbPath());
        if (!sourceFile.exists() || !sourceFile.open(QIODevice::ReadOnly)) {
            throw std::runtime_error("源数据库文件无法读取");
        }
        sourceFile.close();

        // 加锁备份（防止备份时表结构变更）
        manager.metaMutex.lock();
        success = ZipUtils::compressFile(manager.getDbPath(), backupPath);
        manager.metaMutex.unlock(); // 立即释放锁
        resultPath = backupPath;
        LOG_INFO() << "备份完成，路径:" << resultPath << "成功:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "备份任务异常:" << e.what();
        // 确保异常时锁已释放
        if (manager.metaMutex.tryLock()) {
            manager.metaMutex.unlock();
        }
    }
    if (callback) callback(success, resultPath);
    manager.decrementPendingTasks();
}

// 修改字段任务实现
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
            // 加锁修改元数据
            manager.metaMutex.lock();
            success = manager.table->addField(newField);
            manager.metaMutex.unlock(); // 立即释放锁
        }
        if (callback) callback(success, newField.name);
        LOG_DEBUG() << "修改字段完成，字段:" << newField.name << "成功:" << success;
    } catch (const std::exception& e) {
        LOG_ERROR() << "ModifyFieldTask异常:" << e.what() << "字段:" << newField.name;
        // 确保异常时锁已释放
        if (manager.metaMutex.tryLock()) {
            manager.metaMutex.unlock();
        }
        if (callback) callback(false, newField.name);
    }
    manager.decrementPendingTasks();
}

// 数据打包工具函数
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

// 任务计数管理
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

// 外部DynamicTable构造函数
DBManager::DBManager(DynamicTable* externalTable, int maxThreadCount) 
    : dbPath("external_table"), table(externalTable), ownTable(false),
      metaMutex(QMutex::Recursive), dataMutex(QMutex::Recursive) {  // 递归锁支持重入
    if (!externalTable) {
        throw std::invalid_argument("External DynamicTable cannot be null");
    }

    // 线程池初始化
    int optimalThreads = std::max(1, std::min(maxThreadCount, QThread::idealThreadCount()));
    threadPool.setMaxThreadCount(optimalThreads);
    threadPool.setExpiryTimeout(-1); // 永不超时
    LOG_INFO() << "线程池初始化完成，线程数:" << optimalThreads;

    // 从外部表获取字段信息
    baseFields = externalTable->getHeaderParser().fields;
    LOG_INFO() << "使用外部DynamicTable初始化DBManager，字段数量:" << baseFields.size();

    isInitialized = true;
}

// 内部DynamicTable构造函数
DBManager::DBManager(const QString& path, int maxThreadCount) 
    : dbPath(path), table(new DynamicTable()), ownTable(true),
      metaMutex(QMutex::Recursive), dataMutex(QMutex::Recursive) {  // 递归锁支持重入
    // 线程池初始化
    int optimalThreads = std::max(1, std::min(maxThreadCount, QThread::idealThreadCount()));
    threadPool.setMaxThreadCount(optimalThreads);
    threadPool.setExpiryTimeout(-1); // 永不超时
    LOG_INFO() << "线程池初始化完成，线程数:" << optimalThreads;
    
    // 初始化基础字段
    FieldDef idField{TYPE_INT, sizeof(int), "id"};
    FieldDef nameField{TYPE_STRING, FIXED_STRING_LENGTH, "name"};
    FieldDef scoreField{TYPE_FLOAT, sizeof(float), "score"};
    baseFields = {idField, nameField, scoreField};
}

// 析构函数
DBManager::~DBManager() {
    waitForAllTasks();
    threadPool.clear();
    threadPool.waitForDone();
    
    // 释放内部创建的DynamicTable
    if (ownTable && table) {
        delete table;
        table = nullptr;
    }
    LOG_INFO() << "DBManager已销毁";
}

// 初始化或加载数据库
bool DBManager::initOrLoad() {
    metaMutex.lock(); // 元数据锁保护初始化过程
    if (isInitialized) {
        metaMutex.unlock();
        return true;
    }

    // 外部表无需初始化，直接检查状态
    if (!ownTable) {
        isInitialized = table->isTableLoaded();
        metaMutex.unlock();
        return isInitialized;
    }

    // 内部表初始化/加载
    QFileInfo fileInfo(dbPath);
    bool success = false;

    if (fileInfo.exists()) {
        success = table->loadTable(dbPath);
        LOG_INFO() << "尝试加载数据库:" << dbPath << "结果:" << success;
    } else {
        success = table->initTable(dbPath, baseFields);
        LOG_INFO() << "尝试创建数据库:" << dbPath << "结果:" << success;
    }

    if (success) {
        isInitialized = true;
    } else {
        LOG_ERROR() << "数据库初始化失败，路径:" << dbPath;
    }

    metaMutex.unlock();
    return success;
}

// 任务提交接口
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

// 等待所有任务完成
void DBManager::waitForAllTasks() {
    taskMutex.lock();
    while (pendingTasks > 0) {
        LOG_DEBUG() << "等待任务完成，剩余:" << pendingTasks;
        taskCondition.wait(&taskMutex);
    }
    taskMutex.unlock();
}

// 启用每日备份
void DBManager::enableDailyBackup(const QString& backupDir, int keepDays) {
    LOG_INFO() << "启用每日备份，目录:" << backupDir << "保留天数:" << keepDays;
    // 实际项目需实现定时备份逻辑
}

// 实现"路径+线程数"构造函数
DBManager::DBManager(const QString& path, int maxThreadCount) 
    : dbPath(path), table(nullptr), ownTable(true), threadPool(maxThreadCount), pendingTasks(0) {
    // 初始化基础字段（根据业务需求定义，例如id、name、score）
    baseFields = {
        FieldDef(TYPE_INT, sizeof(int), "id"),
        FieldDef(TYPE_STRING, 64, "name"),  // 假设字符串长度64
        FieldDef(TYPE_FLOAT, sizeof(float), "score")
    };
    threadPool.setMaxThreadCount(maxThreadCount);
}

// 实现"外部DynamicTable+线程数"构造函数
DBManager::DBManager(DynamicTable* externalTable, int maxThreadCount)
    : table(externalTable), ownTable(false), threadPool(maxThreadCount), pendingTasks(0) {
    threadPool.setMaxThreadCount(maxThreadCount);
    if (table) {
        // 从外部表获取字段信息
        baseFields = table->getHeaderParser().fields;
    }
}

// 析构函数实现
DBManager::~DBManager() {
    waitForAllTasks();
    if (ownTable && table) {
        delete table;  // 仅当拥有所有权时销毁
    }
}
