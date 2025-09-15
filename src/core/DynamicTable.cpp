#include "DynamicTable.h"
#include <QByteArray>
#include <stdexcept>

// DataValue 实现
DataValue::DataValue() : type(TYPE_INVALID), valueLen(0) {
    memset(strVal, 0, FIXED_STRING_LENGTH);
}

DataValue::DataValue(const DataValue& other) : type(other.type), valueLen(other.valueLen) {
    memset(strVal, 0, FIXED_STRING_LENGTH);
    switch(type) {
        case TYPE_INT: intVal = other.intVal; break;
        case TYPE_FLOAT: floatVal = other.floatVal; break;
        case TYPE_STRING: 
            memcpy(strVal, other.strVal, std::min(valueLen, (size_t)FIXED_STRING_LENGTH));
            break;
        default: break;
    }
}

DataValue& DataValue::operator=(const DataValue& other) {
    if (this != &other) {
        type = other.type;
        valueLen = other.valueLen;
        memset(strVal, 0, FIXED_STRING_LENGTH);
        switch(type) {
            case TYPE_INT: intVal = other.intVal; break;
            case TYPE_FLOAT: floatVal = other.floatVal; break;
            case TYPE_STRING: 
                memcpy(strVal, other.strVal, std::min(valueLen, (size_t)FIXED_STRING_LENGTH));
                break;
            default: break;
        }
    }
    return *this;
}

DataValue::~DataValue() { clear(); }

void DataValue::clear() {
    type = TYPE_INVALID;
    valueLen = 0;
    memset(strVal, 0, FIXED_STRING_LENGTH);
}

// HeaderParser 实现
HeaderParser::HeaderParser() : headerTotalLen(0), recordSize(0), isParsed(false) {}

bool HeaderParser::parseHeader(ISCADA::DB::MappedFileOper& fileOp) {
    if (!fileOp.isOpen()) return false;

    try {
        fields.clear();
        headerTotalLen = 0;
        recordSize = 0;
        isParsed = false;

        // 读取头部总长度
        char lenBuf[sizeof(size_t)];
        fileOp.read(lenBuf, 0, sizeof(size_t));
        headerTotalLen = *reinterpret_cast<size_t*>(lenBuf);

        // 读取字段数量
        uint8_t fieldCount = 0;
        fileOp.read(reinterpret_cast<char*>(&fieldCount), sizeof(size_t), sizeof(uint8_t));

        // 读取每个字段定义
        size_t currentOffset = sizeof(size_t) + sizeof(uint8_t);
        for (uint8_t i = 0; i < fieldCount; ++i) {
            FieldDef field;
            fileOp.read(reinterpret_cast<char*>(&field.type), currentOffset, sizeof(FieldType));
            currentOffset += sizeof(FieldType);

            fileOp.read(reinterpret_cast<char*>(&field.valueLen), currentOffset, sizeof(size_t));
            currentOffset += sizeof(size_t);

            uint8_t nameLen = 0;
            fileOp.read(reinterpret_cast<char*>(&nameLen), currentOffset, sizeof(uint8_t));
            currentOffset += sizeof(uint8_t);

            if (nameLen > 0) {
                char* nameBuffer = new char[nameLen + 1];
                memset(nameBuffer, 0, nameLen + 1);
                fileOp.read(nameBuffer, currentOffset, nameLen);
                field.name = QString::fromUtf8(nameBuffer, nameLen);
                currentOffset += nameLen;
                delete[] nameBuffer;
            }

            if (!field.isValid()) return false;
            fields.push_back(field);
            recordSize += field.valueLen;
        }

        isParsed = (currentOffset == headerTotalLen);
        return isParsed;
    } catch (...) {
        return false;
    }
}

// DynamicTable 实现
DynamicTable::DynamicTable() : isLoaded(false) {}

DynamicTable::~DynamicTable() { close(); }

size_t DynamicTable::calculateRecordSize(const std::vector<FieldDef>& fields) {
    size_t size = 0;
    for (const auto& field : fields) size += field.valueLen;
    return size;
}

bool DynamicTable::initTable(const QString& path, const std::vector<FieldDef>& fields) {
    ScopedMetaLock metaLock(*this);
    if (!metaLock.isLocked()) return false;

    try {
        if (!fileOp.open(path.toStdString(), ISCADA::DB::OpenMode::Create, 1024 * 1024)) {
            return false;
        }

        // 写入表头
        size_t headerLen = sizeof(size_t) + sizeof(uint8_t);
        for (const auto& field : fields) {
            headerLen += sizeof(FieldType) + sizeof(size_t) + sizeof(uint8_t) + field.name.toUtf8().size();
        }

        // 写入头部总长度
        fileOp.write(reinterpret_cast<const char*>(&headerLen), sizeof(size_t));
        // 写入字段数量
        uint8_t fieldCount = fields.size();
        fileOp.write(reinterpret_cast<const char*>(&fieldCount), sizeof(uint8_t));

        // 写入每个字段
        for (const auto& field : fields) {
            fileOp.write(reinterpret_cast<const char*>(&field.type), sizeof(FieldType));
            fileOp.write(reinterpret_cast<const char*>(&field.valueLen), sizeof(size_t));
            
            QByteArray nameBytes = field.name.toUtf8();
            uint8_t nameLen = nameBytes.size();
            fileOp.write(reinterpret_cast<const char*>(&nameLen), sizeof(uint8_t));
            fileOp.write(nameBytes.constData(), nameLen);
        }

        headerParser.fields = fields;
        headerParser.setHeaderTotalLen(headerLen);
        headerParser.recalculateRecordSize();
        fieldMap.clear();
        for (const auto& field : fields) {
            fieldMap[field.name] = field;
        }

        fileOp.close();
        isLoaded = true;
        return true;
    } catch (...) {
        return false;
    }
}

bool DynamicTable::loadTable(const QString& path) {
    ScopedMetaLock metaLock(*this);
    if (!metaLock.isLocked()) return false;

    try {
        if (!fileOp.open(path.toStdString(), ISCADA::DB::OpenMode::ReadWrite, -1)) {
            return false;
        }

        if (!headerParser.parseHeader(fileOp)) {
            fileOp.close();
            return false;
        }

        fieldMap.clear();
        for (const auto& field : headerParser.fields) {
            fieldMap[field.name] = field;
        }

        isLoaded = true;
        return true;
    } catch (...) {
        fileOp.close();
        return false;
    }
}

bool DynamicTable::readRecord(size_t recordIndex, std::map<QString, DataValue>& result) {
    QMutexLocker locker(&tableMutex);  // 内部加锁
    ScopedDataLock dataLock(*this);
    if (!dataLock.isLocked() || !isLoaded) return false;

    size_t headerLen = headerParser.getHeaderTotalLen();
    size_t recordSize = headerParser.getRecordSize();
    size_t recordOffset = headerLen + recordIndex * recordSize;

    if (recordOffset + recordSize > fileOp.getUsedSize()) return false;

    result.clear();
    size_t fieldOffset = 0;
    for (const auto& field : headerParser.fields) {
        DataValue val;
        val.type = field.type;
        val.valueLen = field.valueLen;

        switch (field.type) {
            case TYPE_INT:
                fileOp.read(reinterpret_cast<char*>(&val.intVal), recordOffset + fieldOffset, sizeof(int));
                break;
            case TYPE_FLOAT:
                fileOp.read(reinterpret_cast<char*>(&val.floatVal), recordOffset + fieldOffset, sizeof(float));
                break;
            case TYPE_STRING:
                fileOp.read(val.strVal, recordOffset + fieldOffset, field.valueLen);
                break;
            default:
                return false;
        }

        result[field.name] = val;
        fieldOffset += field.valueLen;
    }
    return true;
}

bool DynamicTable::writeRecord(const std::map<QString, DataValue>& data) {
    QMutexLocker locker(&tableMutex);  // 内部加锁
    ScopedDataLock dataLock(*this);
    if (!dataLock.isLocked() || !isLoaded) return false;
    size_t recordSize = headerParser.getRecordSize();
    

    char* buffer = new char[recordSize];
    memset(buffer, 0, recordSize);
    size_t fieldOffset = 0;

    for (const auto& field : headerParser.fields) {
        auto it = data.find(field.name);
        if (it == data.end()) {
            delete[] buffer;
            return false;
        }

        const DataValue& val = it->second;
        if (val.type != field.type || val.valueLen != field.valueLen) {
            delete[] buffer;
            return false;
        }

        switch (field.type) {
            case TYPE_INT:
                memcpy(buffer + fieldOffset, &val.intVal, sizeof(int));
                break;
            case TYPE_FLOAT:
                memcpy(buffer + fieldOffset, &val.floatVal, sizeof(float));
                break;
            case TYPE_STRING:
                memcpy(buffer + fieldOffset, val.strVal, field.valueLen);
                break;
            default:
                delete[] buffer;
                return false;
        }

        fieldOffset += field.valueLen;
    }

    try {
        fileOp.write(buffer, recordSize);
        delete[] buffer;
        return true;
    } catch (...) {
        delete[] buffer;
        return false;
    }
}

size_t DynamicTable::getRecordCount() {
    if (!isLoaded) return 0;
    size_t dataSize = fileOp.getUsedSize() - ISCADA::DB::MappedFileOper::HEADER_SIZE - headerParser.getHeaderTotalLen();
    return dataSize / headerParser.getRecordSize();
}

void DynamicTable::close() {
    if (isLoaded) {
        fileOp.close();
        isLoaded = false;
    }
}

bool DynamicTable::addField(const FieldDef& newField) {
    ScopedMetaLock metaLock(*this);
    if (!metaLock.isLocked() || !isLoaded || !newField.isValid()) return false;

    if (fieldMap.count(newField.name)) return false;

    size_t originalRecordSize = headerParser.getRecordSize();
    headerParser.fields.push_back(newField);
    headerParser.recalculateRecordSize();
    fieldMap[newField.name] = newField;

    if (!rewriteHeader() || !updateExistingRecords(newField, originalRecordSize)) {
        headerParser.fields.pop_back();
        headerParser.setRecordSize(originalRecordSize);
        fieldMap.erase(newField.name);
        return false;
    }
    return true;
}

bool DynamicTable::rewriteHeader() {
    // 实现表头重写逻辑
    return true;
}

bool DynamicTable::updateExistingRecords(const FieldDef& newField, size_t originalRecordSize) {
    // 实现现有记录更新逻辑
    return true;
}
