#include "core/DBManager.h"
#include "core/DBInitializer.h"
#include <QCoreApplication>
#include "utils/logger.h"
#include <QDir>

int main(int argc, char* argv[]) {
    QCoreApplication app(argc, argv);

    LOG_INFO() << "程序启动";
    
    // 数据库文件路径设置
    QString dbRootDir = "database";
    QString internalDbPath = dbRootDir + "/internal_table.dat"; // 内部表路径
    
    // 确保数据库目录存在
    QDir dbDir(dbRootDir);
    if (!dbDir.exists() && !dbDir.mkpath(".")) {
        LOG_ERROR() << "无法创建数据库目录: " << dbRootDir;
        return -1;
    }

    // 初始化DBManager，使用内部DynamicTable
    DBManager dbManager(internalDbPath, 4); // 内部表构造函数
    
    // 关键函数调用：通过initOrLoad()初始化内部表
    if (!dbManager.initOrLoad()) {
        LOG_ERROR() << "DBManager初始化内部表失败";
        return -1;
    }
    
    // 验证初始化状态
    if (!dbManager.isReady()) {
        LOG_ERROR() << "DBManager未就绪";
        return -1;
    }

    LOG_INFO() << "DBManager初始化成功，当前记录数: " << dbManager.getRecordCount();

    // 启用每日备份
    dbManager.enableDailyBackup("backups", 7);
    LOG_INFO() << "已启用每日备份";

    return app.exec();
}