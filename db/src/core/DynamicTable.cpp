#include "DynamicTable.h"
#include <QByteArray>
#include <stdexcept>
#include "utils/logger.h"
#include <thread>

// DataValue ʵ��
DataValue::DataValue() : type(TYPE_INVALID), valueLen(0) {
    memset(strVal, 0, FIXED_STRING_LENGTH);
}

DataValue::DataValue(FieldType t, size_t len) : type(t), valueLen(len) {
    // Initialize union appropriately depending on type
    memset(strVal, 0, FIXED_STRING_LENGTH);
    switch (type) {
        case TYPE_INT: intVal = 0; break;
        case TYPE_FLOAT: floatVal = 0.0f; break;
        case TYPE_STRING: /* buffer already zeroed */ break;
        default: break;
    }
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

// HeaderParser ʵ��
HeaderParser::HeaderParser() : headerTotalLen(0), recordSize(0), isParsed(false) {}

bool HeaderParser::parseHeader(ISCADA::DB::MappedFileOper& fileOp) {
    if (!fileOp.isOpen()) return false;

    try {
        fields.clear();
        headerTotalLen = 0;
        recordSize = 0;
        isParsed = false;

        // ��ȡͷ���ܳ���
        char lenBuf[sizeof(size_t)];
        fileOp.read(lenBuf, 0, sizeof(size_t));
        headerTotalLen = *reinterpret_cast<size_t*>(lenBuf);

        // ��ȡ�ֶ�����
        uint8_t fieldCount = 0;
        fileOp.read(reinterpret_cast<char*>(&fieldCount), sizeof(size_t), sizeof(uint8_t));

        // ��ȡÿ���ֶζ���
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

// DynamicTable ʵ��
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

        // д���ͷ
        size_t headerLen = sizeof(size_t) + sizeof(uint8_t);
        for (const auto& field : fields) {
            headerLen += sizeof(FieldType) + sizeof(size_t) + sizeof(uint8_t) + field.name.toUtf8().size();
        }

        // д��ͷ���ܳ���
        fileOp.write(reinterpret_cast<const char*>(&headerLen), sizeof(size_t));
        // д���ֶ�����
        uint8_t fieldCount = fields.size();
        fileOp.write(reinterpret_cast<const char*>(&fieldCount), sizeof(uint8_t));

        // д��ÿ���ֶ�
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

    // Keep the mapped file open so subsequent read/write operations can
    // use the mapping. Closing here caused later writeRecord calls to
    // fail with "fileOp not open".
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
    QMutexLocker locker(&tableMutex);  // �ڲ�����
    ScopedDataLock dataLock(*this);
    if (!dataLock.isLocked() || !isLoaded) return false;

    size_t headerLen = headerParser.getHeaderTotalLen();
    size_t recordSize = headerParser.getRecordSize();
    size_t recordOffset = headerLen + recordIndex * recordSize;

    // Ensure the requested record exists within used_size
    if (recordOffset + recordSize > fileOp.getUsedSize() - ISCADA::DB::MappedFileOper::HEADER_SIZE) return false;

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
    QMutexLocker locker(&tableMutex);  // �ڲ�����
    ScopedDataLock dataLock(*this);
    if (!dataLock.isLocked() || !isLoaded) return false;

    size_t headerLen = headerParser.getHeaderTotalLen();
    size_t recordSize = headerParser.getRecordSize();
    size_t recordOffset = fileOp.getUsedSize() - ISCADA::DB::MappedFileOper::HEADER_SIZE;

    char* buffer = new char[recordSize];
    memset(buffer, 0, recordSize);
    size_t fieldOffset = 0;

    for (const auto& field : headerParser.fields) {
        auto it = data.find(field.name);
        if (it == data.end()) {
            delete[] buffer;
            qWarning("DynamicTable::writeRecord failed: missing field '%s'", field.name.toUtf8().constData());
            return false;
        }

        const DataValue& val = it->second;
        if (val.type != field.type || val.valueLen != field.valueLen) {
            delete[] buffer;
            qWarning("DynamicTable::writeRecord failed: type/length mismatch for field '%s' (expected type=%d len=%zu, got type=%d len=%zu)",
                     field.name.toUtf8().constData(), static_cast<int>(field.type), field.valueLen,
                     static_cast<int>(val.type), val.valueLen);
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
    // Diagnostic: log computed offsets and sizes before write
        LOG_DEBUG() << QString("DynamicTable::writeRecord(): isOpen=%1, used_size=%2, headerTotalLen=%3, recordSize=%4, recordOffset=%5, writeLen=%6")
                        .arg(fileOp.isOpen() ? 1 : 0)
                        .arg((unsigned long long)fileOp.getUsedSize())
                        .arg((unsigned long long)headerLen)
                        .arg((unsigned long long)recordSize)
                        .arg((unsigned long long)recordOffset)
                        .arg((unsigned long long)recordSize);
#ifdef ENABLE_RECORD_TRACE
    LOG_DEBUG() << "TRACE DynamicTable::writeRecord: recordOffset=" << recordOffset
            << " recordSize=" << recordSize << " thread=" << std::this_thread::get_id();
#endif

        // Boundary checks
        if (!fileOp.isOpen()) {
            LOG_WARN() << QString("DynamicTable::writeRecord failed: fileOp not open");
            delete[] buffer;
            return false;
        }

        // Ensure mapping has capacity before writing
        if (!fileOp.ensureCapacity(recordSize)) {
            LOG_WARN() << QString("DynamicTable::writeRecord exception: Write would exceed file bounds (used=%1, len=%2, fileSize=%3)")
                          .arg((unsigned long long)fileOp.getUsedSize())
                          .arg((unsigned long long)recordSize)
                          .arg((unsigned long long)fileOp.getSize());
            delete[] buffer;
            return false;
        }

    fileOp.write(buffer, recordSize);
#ifdef ENABLE_RECORD_TRACE
    LOG_DEBUG() << "TRACE DynamicTable::writeRecord completed: recordOffset=" << recordOffset
            << " writeLen=" << recordSize << " thread=" << std::this_thread::get_id();
#endif
    delete[] buffer;
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR() << QString("DynamicTable::writeRecord exception: %1").arg(e.what());
        delete[] buffer;
        return false;
    } catch (...) {
        LOG_ERROR() << QString("DynamicTable::writeRecord unknown exception");
        delete[] buffer;
        return false;
    }
}

bool DynamicTable::writeRecordAt(size_t recordIndex, const std::map<QString, DataValue>& data) {
    QMutexLocker locker(&tableMutex);
    ScopedDataLock dataLock(*this);
    if (!dataLock.isLocked() || !isLoaded) return false;

    size_t headerLen = headerParser.getHeaderTotalLen();
    size_t recordSize = headerParser.getRecordSize();
    size_t recordOffset = headerLen + recordIndex * recordSize;

    char* buffer = new char[recordSize];
    memset(buffer, 0, recordSize);
    size_t fieldOffset = 0;

    for (const auto& field : headerParser.fields) {
        auto it = data.find(field.name);
        if (it == data.end()) {
            delete[] buffer;
            qWarning("DynamicTable::writeRecordAt failed: missing field '%s'", field.name.toUtf8().constData());
            return false;
        }

        const DataValue& val = it->second;
        if (val.type != field.type || val.valueLen != field.valueLen) {
            delete[] buffer;
            qWarning("DynamicTable::writeRecordAt failed: type/length mismatch for field '%s'", field.name.toUtf8().constData());
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
        if (!fileOp.isOpen()) {
            LOG_WARN() << "DynamicTable::writeRecordAt failed: fileOp not open";
            delete[] buffer;
            return false;
        }

    // Boundary check
        if (recordOffset + recordSize > fileOp.getSize()) {
            LOG_WARN() << "DynamicTable::writeRecordAt: recordOffset out of file bounds";
            delete[] buffer;
            return false;
        }

#ifdef ENABLE_RECORD_TRACE
    LOG_DEBUG() << "TRACE DynamicTable::writeRecordAt: index=" << recordIndex
            << " recordOffset=" << recordOffset << " writeLen=" << recordSize
            << " thread=" << std::this_thread::get_id();
#endif
    fileOp.writeAt(buffer, recordOffset, recordSize);
#ifdef ENABLE_RECORD_TRACE
    LOG_DEBUG() << "TRACE DynamicTable::writeRecordAt completed: index=" << recordIndex
            << " recordOffset=" << recordOffset << " writeLen=" << recordSize
            << " thread=" << std::this_thread::get_id();
#endif
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
    // ʵ�ֱ�ͷ��д�߼�
    return true;
}

bool DynamicTable::updateExistingRecords(const FieldDef& newField, size_t originalRecordSize) {
    // ʵ�����м�¼�����߼�
    return true;
}
