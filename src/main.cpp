#include "core/DBManager.h"
#include "core/DBInitializer.h"
#include <QCoreApplication>
#include "utils/logger.h"
#include <QDir>

int main(int argc, char* argv[]) {
    QCoreApplication app(argc, argv);

    LOG_INFO() << "��������";
    
    // ���ݿ��ļ�·������
    QString dbRootDir = "database";
    QString internalDbPath = dbRootDir + "/internal_table.dat"; // �ڲ���·��
    
    // ȷ�����ݿ�Ŀ¼����
    QDir dbDir(dbRootDir);
    if (!dbDir.exists() && !dbDir.mkpath(".")) {
        LOG_ERROR() << "�޷��������ݿ�Ŀ¼: " << dbRootDir;
        return -1;
    }

    // ��ʼ��DBManager��ʹ���ڲ�DynamicTable��
    DBManager dbManager(internalDbPath, 4); // �ڲ����캯��
    
    // �ؼ���������ʽ����initOrLoad()��ʼ���ڲ���
    if (!dbManager.initOrLoad()) {
        LOG_ERROR() << "DBManager��ʼ���ڲ���ʧ��";
        return -1;
    }
    
    // ��֤��ʼ��״̬
    if (!dbManager.isReady()) {
        LOG_ERROR() << "DBManagerδ����";
        return -1;
    }

    LOG_INFO() << "DBManager��ʼ���ɹ�����ǰ��¼��: " << dbManager.getRecordCount();

    // ����ÿ�ձ���
    dbManager.enableDailyBackup("backups", 7);
    LOG_INFO() << "������ÿ�ձ���";

    return app.exec();
}
