#ifndef DYNAMIC_TABLE_H
#define DYNAMIC_TABLE_H

#include "MappedFileOper.h"
#include <vector>
#include <map>
#include <cstring>
#include <cstdint>
#include <QMutex>
#include <QString>
#include <QDebug>

#define FIXED_STRING_LENGTH 128
#define INVALID_FILE_SIZE (size_t)-1

enum FieldType {
    TYPE_INT = 0,
    TYPE_FLOAT = 1,
    TYPE_STRING = 2,
    TYPE_INVALID = 0xFF
};


struct FieldDef {
    FieldType type;
    size_t valueLen;
    QString name;

    FieldDef() : type(TYPE_INVALID), valueLen(0) {}
    FieldDef(FieldType t, size_t len, const QString& n) 
        : type(t), valueLen(len), name(n) {}

    // �� isValid() ���������ṹ���ڲ�
    bool isValid() const {
        if (type == TYPE_INVALID) return false;
        if (name.isEmpty()) return false;
        
        switch(type) {
            case TYPE_INT: return valueLen == sizeof(int);
            case TYPE_FLOAT: return valueLen == sizeof(float);
            case TYPE_STRING: return valueLen <= FIXED_STRING_LENGTH;
            default: return false;
        }
    }
};  // �ṹ���ڴ˴��պ�

struct DataValue {
    FieldType type;
    union {
        int intVal;
        float floatVal;
        char strVal[FIXED_STRING_LENGTH];
    };
    size_t valueLen;

    DataValue(FieldType t, size_t len);
    DataValue();
    DataValue(const DataValue& other);
    DataValue& operator=(const DataValue& other);
    ~DataValue();

    void clear();
};

class HeaderParser {
public:
    std::vector<FieldDef> fields;
private:
    size_t headerTotalLen;
    size_t recordSize;
    bool isParsed;

public:
    HeaderParser();
    bool parseHeader(ISCADA::DB::MappedFileOper& fileOp);
    size_t getHeaderTotalLen() const { return headerTotalLen; }
    void setHeaderTotalLen(size_t len) { headerTotalLen = len; }
    size_t getRecordSize() const { return recordSize; }
    bool isHeaderValid() const { return isParsed && headerTotalLen > 0 && recordSize > 0; }
    void recalculateRecordSize() {
        recordSize = 0;
        for (const auto& field : fields) {
            recordSize += field.valueLen;
        }
    }
    void setRecordSize(size_t size) { recordSize = size; }
};

class DynamicTable {
private:
    QMutex tableMutex;  // �����ڲ����ݽṹ����
    ISCADA::DB::MappedFileOper fileOp;
    HeaderParser headerParser;
    std::map<QString, FieldDef> fieldMap;
    bool isLoaded;
    QMutex metaMutex;
    QMutex dataMutex;

    class ScopedMetaLock {
    public:
        explicit ScopedMetaLock(DynamicTable& table) 
            : table_(table), locked_(table.metaMutex.tryLock(5000)) {
            if (!locked_) qWarning() << "Ԫ��������ʱ��5�룩";
        }
        ~ScopedMetaLock() { if (locked_) table_.metaMutex.unlock(); }
        bool isLocked() const { return locked_; }
    private:
        DynamicTable& table_;
        bool locked_;
    };

    class ScopedDataLock {
    public:
        explicit ScopedDataLock(DynamicTable& table) 
            : table_(table), locked_(table.dataMutex.tryLock(5000)) {
            if (!locked_) qWarning() << "��������ʱ��5�룩";
        }
        ~ScopedDataLock() { if (locked_) table_.dataMutex.unlock(); }
        bool isLocked() const { return locked_; }
    private:
        DynamicTable& table_;
        bool locked_;
    };

    size_t calculateRecordSize(const std::vector<FieldDef>& fields);
    bool rewriteHeader();
    bool updateExistingRecords(const FieldDef& newField, size_t originalRecordSize);

public:
    DynamicTable();
    ~DynamicTable();

    bool initTable(const QString& path, const std::vector<FieldDef>& fields);
    bool loadTable(const QString& path);
    bool readRecord(size_t recordIndex, std::map<QString, DataValue>& result);
    bool writeRecord(const std::map<QString, DataValue>& data);
    bool writeRecordAt(size_t recordIndex, const std::map<QString, DataValue>& data);
    const FieldDef* getFieldDef(const QString& fieldName) const;
    size_t getRecordCount();
    void close();
    const HeaderParser& getHeaderParser() const { return headerParser; }
    bool isTableLoaded() const { return isLoaded; }
    bool addField(const FieldDef& newField);
};

#endif
