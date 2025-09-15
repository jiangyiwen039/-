#ifndef DB_MANAGER_H
#define DB_MANAGER_H

#include "DynamicTable.h"
#include <QThreadPool>
#include <QMutex>
#include <QWaitCondition>
#include <atomic>
#include <functional>
#include <openssl/aes.h>
#include <QMutex>

class DBManager {
public:
    enum CryptoType { ENCRYPT, DECRYPT };

    // 原有构造函数：内部创建DynamicTable
    DBManager(const QString& path, int maxThreadCount);
    // 新增构造函数：接受外部传入的DynamicTable*
    DBManager(DynamicTable* externalTable, int maxThreadCount);
    ~DBManager();

    DynamicTable* getTable() const { return table; }
    bool initOrLoad();
    bool isReady() const { return isInitialized.load(); }
    size_t getRecordCount() { return table->getRecordCount(); }  // 修改为指针访问
    void submitReadTask(size_t index, std::function<void(bool, const std::map<QString, DataValue>&)> callback);
    void submitWriteTask(int id, const QString& name, float score, std::function<void(bool, int)> callback);
    void submitCryptoTask(size_t index, CryptoType type, std::function<void(bool, size_t)> callback);
    void submitBackupTask(const QString& path, std::function<void(bool, const QString&)> callback);
    void submitModifyFieldTask(const FieldDef& field, std::function<void(bool, const QString&)> callback);
    void waitForAllTasks();
    void enableDailyBackup(const QString& backupDir, int keepDays);
    QString getDbPath() const { return dbPath; }

private: 
    QMutex dataMutex;          // ������
    QMutex metaMutex;          // ���
    QMutex taskMutex;          
    QMutex initMutex;          
    QWaitCondition taskCondition;
    QString dbPath;
    DynamicTable* table;
    bool ownTable;             // ������
    QThreadPool threadPool;
    std::vector<FieldDef> baseFields;
    std::atomic<bool> isInitialized{false};
    int pendingTasks;

    class Task {
    public:
        virtual ~Task() = default;
        virtual void run() = 0;
    };

    class ReadTask : public Task, public QRunnable {
    public:
        ReadTask(DBManager& mgr, size_t index, std::function<void(bool, const std::map<QString, DataValue>&)> cb);
        void run() override;
    private:
        DBManager& manager;
        size_t recordIndex;
        std::function<void(bool, const std::map<QString, DataValue>&)> callback;
    };

    class WriteTask : public Task, public QRunnable {
    public:
        WriteTask(DBManager& mgr, int id, const QString& name, float score, std::function<void(bool, int)> cb);
        void run() override;
    private:
        DBManager& manager;
        int id;
        QString name;
        float score;
        std::function<void(bool, int)> callback;
    };

    class CryptoTask : public Task, public QRunnable {
    public:
        CryptoTask(DBManager& mgr, size_t index, CryptoType t, std::function<void(bool, size_t)> cb);
        void run() override;
    private:
        DBManager& manager;
        size_t recordIndex;
        CryptoType type;
        std::function<void(bool, size_t)> callback;
    };

    class BackupTask : public Task, public QRunnable {
    public:
        BackupTask(DBManager& mgr, const QString& path, std::function<void(bool, const QString&)> cb);
        void run() override;
    private:
        DBManager& manager;
        QString backupPath;
        std::function<void(bool, const QString&)> callback;
    };

    class ModifyFieldTask : public Task, public QRunnable {
    public:
        ModifyFieldTask(DBManager& mgr, const FieldDef& field, std::function<void(bool, const QString&)> cb);
        void run() override;
    private:
        DBManager& manager;
        FieldDef newField;
        std::function<void(bool, const QString&)> callback;
    };
    QString dbPath;
    DynamicTable* table;  // 改为指针类型
    bool ownTable;        // 所有权标记移到table后面
    QThreadPool threadPool;
    std::vector<FieldDef> baseFields;
    std::atomic<bool> isInitialized{false};
    int pendingTasks;

    // AES加密相关
    static const unsigned char AES_ENCRYPT_KEY[];
    static const int AES_KEY_LENGTH = 16; // 128位密钥
    static void aesEncrypt(const char* input, char* output, size_t len);
    static void aesDecrypt(const char* input, char* output, size_t len);

    void incrementPendingTasks();
    void decrementPendingTasks();
    static void packIntValue(std::map<QString, DataValue>& data, const QString& key, int value);
    static void packStringValue(std::map<QString, DataValue>& data, const QString& key, const QString& value);
    static void packFloatValue(std::map<QString, DataValue>& data, const QString& key, float value);
};

#endif // DB_MANAGER_H
