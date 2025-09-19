// db/src/core/DBInitializer.cpp
#include "DBInitializer.h"
#include "MappedFileOper.h"
#include <QFile>
#include <QDir>
#include <QJsonArray>
#include <QJsonObject>
#include <QDebug>
#include <stdexcept>
#include <memory>  // 包含智能指针头文件

DBInitializer::DBInitializer(const QString& configPath, const QString& dbRootDir)
    : configPath(configPath), dbRootDir(dbRootDir) {
    // 确保数据库目录存在
    QDir dbDir(dbRootDir);
    if (!dbDir.exists() && !dbDir.mkpath(".")) {
        throw std::runtime_error("无法创建数据库目录: " + dbRootDir.toStdString());
    }
}

bool DBInitializer::start() {
    // 1. 加载配置文件
    if (!loadConfigFile()) {
        qCritical() << "配置文件加载失败，启动中止";
        return false;
    }

    // 2. 解析配置并处理每张表
    QJsonArray tablesArray = configDoc.object()["tables"].toArray();
    for (const auto& tableItem : tablesArray) {
        QJsonObject tableObj = tableItem.toObject();
        QString tableName = tableObj["name"].toString();
        QString tableAlias = tableObj["alias"].toString();
        QString tablePath = dbRootDir + "/" + tableAlias;

        if (tableName.isEmpty() || tableAlias.isEmpty()) {
            qWarning() << "跳过无效表配置（表名或别名为空）";
            continue;
        }

        // 3. 转换配置字段为FieldDef
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
                qWarning() << "表" << tableName << "包含无效字段类型:" << typeStr;
                fieldsValid = false;
                break;
            }

            field.valueLen = fieldObj["valueLen"].toInt();
            if (!field.isValid()) {
                qWarning() << "表" << tableName << "字段" << field.name << "配置无效";
                fieldsValid = false;
                break;
            }
            configFields.push_back(field);
        }

        if (!fieldsValid) continue;

        // 4. 检查文件是否存在并处理
        QFileInfo tableFileInfo(tablePath);
        if (tableFileInfo.exists()) {
            // 4.1 存在表文件：加载并验证
            auto table = std::make_unique<DynamicTable>();
            if (!table->loadTable(tablePath)) {
                qWarning() << "表" << tableName << "加载失败，跳过该表";
                continue;
            }
            
            if (!validateExistingTable(tableAlias, *table, configFields)) {
                qWarning() << "表" << tableName << "结构与配置不一致，跳过该表";
                continue;
            }
            tables[tableAlias] = std::move(table);  // 使用移动语义
            qInfo() << "表" << tableName << "加载并验证通过";
        } else {
            // 4.2 不存在表文件：初始化新表
            if (initNewTable(tableName, tablePath, configFields)) {
                auto table = std::make_unique<DynamicTable>();
                if (table->loadTable(tablePath)) {
                    tables[tableAlias] = std::move(table);  // 使用移动语义
                    qInfo() << "表" << tableName << "初始化成功";
                }
            } else {
                qWarning() << "表" << tableName << "初始化失败";
            }
        }
    }

    return !tables.empty();
}

bool DBInitializer::loadConfigFile() {
    QFile configFile(configPath);
    if (!configFile.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qCritical() << "无法打开配置文件:" << configFile.errorString();
        return false;
    }

    QByteArray configData = configFile.readAll();
    configDoc = QJsonDocument::fromJson(configData);
    if (configDoc.isNull()) {
        qCritical() << "配置文件解析失败（无效JSON格式）";
        return false;
    }

    if (!configDoc.object().contains("tables") || !configDoc.object()["tables"].isArray()) {
        qCritical() << "配置文件缺少有效的tables数组";
        return false;
    }
    return true;
}

bool DBInitializer::validateExistingTable(const QString& tableAlias, const DynamicTable& table,
                                         const std::vector<FieldDef>& configFields) {
    const HeaderParser& header = table.getHeaderParser();
    if (header.fields.size() != configFields.size()) {
        qWarning() << "字段数量不匹配（配置:" << configFields.size() 
                   << "实际:" << header.fields.size() << ")";
        return false;
    }

    // 验证每个字段的类型、长度和名称
    for (size_t i = 0; i < configFields.size(); ++i) {
        const FieldDef& configField = configFields[i];
        const FieldDef& actualField = header.fields[i];

        if (configField.name != actualField.name) {
            qWarning() << "字段名称不匹配（配置:" << configField.name 
                       << "实际:" << actualField.name << ")";
            return false;
        }

        if (configField.type != actualField.type) {
            qWarning() << "字段" << configField.name << "类型不匹配";
            return false;
        }

        if (configField.valueLen != actualField.valueLen) {
            qWarning() << "字段" << configField.name << "长度不匹配（配置:" 
                       << configField.valueLen << "实际:" << actualField.valueLen << ")";
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
