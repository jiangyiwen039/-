// db/src/core/DBInitializer.cpp
#include "DBInitializer.h"
#include "MappedFileOper.h"
#include <QFile>
#include <QDir>
#include <QJsonArray>
#include <QJsonObject>
#include <QDebug>
#include <stdexcept>
#include <memory>  // ��������ָ��ͷ�ļ�

DBInitializer::DBInitializer(const QString& configPath, const QString& dbRootDir)
    : configPath(configPath), dbRootDir(dbRootDir) {
    // ȷ�����ݿ�Ŀ¼����
    QDir dbDir(dbRootDir);
    if (!dbDir.exists() && !dbDir.mkpath(".")) {
        throw std::runtime_error("�޷��������ݿ�Ŀ¼: " + dbRootDir.toStdString());
    }
}

bool DBInitializer::start() {
    // 1. ���������ļ�
    if (!loadConfigFile()) {
        qCritical() << "�����ļ�����ʧ�ܣ�������ֹ";
        return false;
    }

    // 2. �������ò�����ÿ�ű�
    QJsonArray tablesArray = configDoc.object()["tables"].toArray();
    for (const auto& tableItem : tablesArray) {
        QJsonObject tableObj = tableItem.toObject();
        QString tableName = tableObj["name"].toString();
        QString tableAlias = tableObj["alias"].toString();
        QString tablePath = dbRootDir + "/" + tableAlias;

        if (tableName.isEmpty() || tableAlias.isEmpty()) {
            qWarning() << "������Ч�����ã����������Ϊ�գ�";
            continue;
        }

        // 3. ת�������ֶ�ΪFieldDef
        std::vector<FieldDef> configFields;
        QJsonArray fieldsArray = tableObj["fields"].toArray();
        bool fieldsValid = true;
        for (const auto& fieldItem : fieldsArray) {
            QJsonObject fieldObj = fieldItem.toObject();
            FieldDef field;
            field.name = fieldObj["name"].toString();
            
            QString typeStr = fieldObj["type"].toString();
            if (typeStr == "int") field.type = TYPE_INT;
            else if (typeStr == "float") field.type = TYPE_FLOAT;
            else if (typeStr == "string") field.type = TYPE_STRING;
            else {
                qWarning() << "��" << tableName << "������Ч�ֶ�����:" << typeStr;
                fieldsValid = false;
                break;
            }

            field.valueLen = fieldObj["valueLen"].toInt();
            if (!field.isValid()) {
                qWarning() << "��" << tableName << "�ֶ�" << field.name << "������Ч";
                fieldsValid = false;
                break;
            }
            configFields.push_back(field);
        }

        if (!fieldsValid) continue;

        // 4. ����ļ��Ƿ���ڲ�����
        QFileInfo tableFileInfo(tablePath);
        if (tableFileInfo.exists()) {
            // 4.1 ���ڱ��ļ������ز���֤
            auto table = std::make_unique<DynamicTable>();
            if (!table->loadTable(tablePath)) {
                qWarning() << "��" << tableName << "����ʧ�ܣ������ñ�";
                continue;
            }
            
            if (!validateExistingTable(tableAlias, *table, configFields)) {
                qWarning() << "��" << tableName << "�ṹ�����ò�һ�£������ñ�";
                continue;
            }
            tables[tableAlias] = std::move(table);  // ʹ���ƶ�����
            qInfo() << "��" << tableName << "���ز���֤ͨ��";
        } else {
            // 4.2 �����ڱ��ļ�����ʼ���±�
            if (initNewTable(tableName, tablePath, configFields)) {
                auto table = std::make_unique<DynamicTable>();
                if (table->loadTable(tablePath)) {
                    tables[tableAlias] = std::move(table);  // ʹ���ƶ�����
                    qInfo() << "��" << tableName << "��ʼ���ɹ�";
                }
            } else {
                qWarning() << "��" << tableName << "��ʼ��ʧ��";
            }
        }
    }

    return !tables.empty();
}

bool DBInitializer::loadConfigFile() {
    QFile configFile(configPath);
    if (!configFile.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qCritical() << "�޷��������ļ�:" << configFile.errorString();
        return false;
    }

    QByteArray configData = configFile.readAll();
    configDoc = QJsonDocument::fromJson(configData);
    if (configDoc.isNull()) {
        qCritical() << "�����ļ�����ʧ�ܣ���ЧJSON��ʽ��";
        return false;
    }

    if (!configDoc.object().contains("tables") || !configDoc.object()["tables"].isArray()) {
        qCritical() << "�����ļ�ȱ����Ч��tables����";
        return false;
    }
    return true;
}

bool DBInitializer::validateExistingTable(const QString& tableAlias, const DynamicTable& table,
                                         const std::vector<FieldDef>& configFields) {
    const HeaderParser& header = table.getHeaderParser();
    if (header.fields.size() != configFields.size()) {
        qWarning() << "�ֶ�������ƥ�䣨����:" << configFields.size() 
                   << "ʵ��:" << header.fields.size() << ")";
        return false;
    }

    // ��֤ÿ���ֶε����͡����Ⱥ�����
    for (size_t i = 0; i < configFields.size(); ++i) {
        const FieldDef& configField = configFields[i];
        const FieldDef& actualField = header.fields[i];

        if (configField.name != actualField.name) {
            qWarning() << "�ֶ����Ʋ�ƥ�䣨����:" << configField.name 
                       << "ʵ��:" << actualField.name << ")";
            return false;
        }

        if (configField.type != actualField.type) {
            qWarning() << "�ֶ�" << configField.name << "���Ͳ�ƥ��";
            return false;
        }

        if (configField.valueLen != actualField.valueLen) {
            qWarning() << "�ֶ�" << configField.name << "���Ȳ�ƥ�䣨����:" 
                       << configField.valueLen << "ʵ��:" << actualField.valueLen << ")";
            return false;
        }
    }
    return true;
}

bool DBInitializer::initNewTable(const QString& tableName, const QString& tablePath,
                                const std::vector<FieldDef>& fields) {
    DynamicTable table;
    return table.initTable(tablePath, fields);
}
