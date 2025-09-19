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
    // ���캯�������������ļ�·�������ݿ��Ŀ¼
    DBInitializer(const QString& configPath, const QString& dbRootDir);
    
    // �������ݿ���ķ���
    bool start();
    
    // ��ȡ���غ�ı���
    std::map<QString, std::unique_ptr<DynamicTable>>& getTables() { return tables; }

private:
    // ��ȡ������JSON�����ļ�
    bool loadConfigFile();
    
    // ��֤���б�ṹ������һ����
    bool validateExistingTable(const QString& tableAlias, const DynamicTable& table, 
                             const std::vector<FieldDef>& configFields);
    
    // �������ó�ʼ���±�
    bool initNewTable(const QString& tableName, const QString& tableAlias, 
                     const std::vector<FieldDef>& fields);

private:
    QString configPath;          // �����ļ�·��
    QString dbRootDir;           // ���ݿ��ļ���Ŀ¼
    QJsonDocument configDoc;     // ������������ĵ�
    std::map<QString, std::unique_ptr<DynamicTable>> tables; // ���������ʵ����ӳ��
};

#endif // DB_INITIALIZER_H
