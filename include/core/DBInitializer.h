#ifndef DB_INITIALIZER_H
#define DB_INITIALIZER_H

#include "DynamicTable.h"
#include <QString>
#include <QJsonDocument>
#include <vector>
#include <map>
#include <memory>

class DBInitializer {
public:
    // 构造函数：传入配置文件路径和数据库根目录
    DBInitializer(const QString& configPath, const QString& dbRootDir);
    
    // 启动数据库核心方法
    bool start();
    
    // 获取加载后的表集合
    std::map<QString, std::unique_ptr<DynamicTable>>& getTables() { return tables; }

private:
    // 读取并解析JSON配置文件
    bool loadConfigFile();
    
    // 校验已有表结构与配置一致性
    bool validateExistingTable(const QString& tableAlias, const DynamicTable& table, 
                             const std::vector<FieldDef>& configFields);
    
    // 根据配置初始化新表
    bool initNewTable(const QString& tableName, const QString& tableAlias, 
                     const std::vector<FieldDef>& fields);

private:
    QString configPath;          // 配置文件路径
    QString dbRootDir;           // 数据库文件根目录
    QJsonDocument configDoc;     // 解析后的配置文档
     std::map<QString, std::unique_ptr<DynamicTable>> tables;// 表别名到表实例的映射
};

#endif // DB_INITIALIZER_H
