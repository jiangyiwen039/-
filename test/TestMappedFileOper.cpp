#include <iostream>
#include <vector>
#include "core/MappedFileOper.h"
#include "core/common.h"
#include <QString>
#include <map>
#include <cstring>
#include "core/DynamicTable.h"
const int FIXED_CHECK_INTERVAL = 1000; 

using namespace ISCADA::Test;
using namespace ISCADA::DB;

MappedFileOper mfo(0.5, std::chrono::milliseconds(FIXED_CHECK_INTERVAL));

bool initMappedFile(std::vector<std::string> args) {
    if (args.empty()) {
        std::cerr << "错误：缺少文件路径参数" << std::endl;
        return false;
    }
    try {
        bool flag = mfo.open(args[0], OpenMode::Create, 1024 * 1024);
        std::cout << (flag ? "初始化成功" : "初始化失败") << std::endl;
        return flag;
    } catch (const std::exception& e) {
        std::cerr << "初始化失败：" << e.what() << std::endl;
        return false;
    }
}

bool writeMappedFile(std::vector<std::string> args) {
    if (args.empty()) {
        std::cerr << "错误：缺少文件路径参数" << std::endl;
        return false;
    }
    try {
        std::string content = "Test content";
        if (!mfo.isOpen()) {
            mfo.open(args[0], OpenMode::ReadWrite, -1);
        }
        for (int i = 0; i < 2000; ++i) {
            mfo.write(content.data(), content.size());
        }
        mfo.close();
        std::cout << "写入完成" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "写入失败：" << e.what() << std::endl;
        return false;
    }
}

bool readMappedFile(std::vector<std::string> args) {
    if (args.size() < 2) {
        std::cerr << "错误：缺少文件路径或偏移量参数" << std::endl;
        return false;
    }
    try {
        long offset = std::stol(args[1]);
        char buffer[64] = {0};
        if (!mfo.isOpen()) {
            mfo.open(args[0], OpenMode::ReadOnly, -1);
        }
        mfo.read(buffer, offset, 63);
        std::cout << "读取内容: " << buffer << std::endl;
        mfo.close();
        return true;
    } catch (const std::invalid_argument& e) {
        std::cerr << "偏移量参数错误：" << e.what() << std::endl;
        return false;
    } catch (const std::out_of_range& e) {
        std::cerr << "读取范围错误：" << e.what() << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "读取失败：" << e.what() << std::endl;
        return false;
    }
}

bool testDynamicTable(std::vector<std::string> args) {
    if (args.empty()) {
        std::cerr << "错误：缺少文件路径参数" << std::endl;
        return false;
    }
    try {
        DynamicTable table;
        std::vector<FieldDef> fields = {
            {TYPE_INT, sizeof(int), "id"},
            {TYPE_STRING, 32, "name"}
        };
        if (!table.initTable(QString::fromStdString(args[0]), fields)) {
            std::cerr << "动态表初始化失败" << std::endl;
            return false;
        }

        std::map<QString, DataValue> data;
        DataValue idVal{TYPE_INT, sizeof(int)};
        idVal.intVal = 1;
        data["id"] = idVal;

        DataValue nameVal{TYPE_STRING, 32};
        strcpy(nameVal.strVal, "test");
        data["name"] = nameVal;

        bool success = table.writeRecord(data);
        std::cout << (success ? "动态表写入成功" : "动态表写入失败") << std::endl;
        return success;
    } catch (const std::exception& e) {
        std::cerr << "动态表测试失败：" << e.what() << std::endl;
        return false;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "用法: " << argv[0] << " [命令] [参数...]" << std::endl;
        std::cerr << "命令列表:" << std::endl;
        std::cerr << "  -c, --create   创建内存映射文件" << std::endl;
        std::cerr << "  -w, --write    写入文件" << std::endl;
        std::cerr << "  -r, --read     读取文件（参数：偏移量）" << std::endl;
        std::cerr << "  -t, --table    测试动态表" << std::endl;
        return 1;
    }

    std::vector<Command> commands = {
        {{"-c", "--create"}, "创建内存映射文件", initMappedFile, 1},
        {{"-w", "--write"}, "写入文件", writeMappedFile, 1},
        {{"-r", "--read"}, "读取文件", readMappedFile, 2},
        {{"-t", "--table"}, "测试动态表", testDynamicTable, 1}
    };

    std::string cmdName = argv[1];
    std::vector<std::string> args;
    for (int i = 2; i < argc; ++i) {
        args.push_back(argv[i]);
    }

    for (const auto& cmd : commands) {
        for (const auto& name : cmd.options) {
            if (name == cmdName) {
                if (args.size() < cmd.argCount) {
                    std::cerr << "错误：参数数量不足" << std::endl;
                    return 1;
                }
                cmd.action(args);
                return 0;
            }
        }
    }

    std::cerr << "错误：未知命令" << std::endl;
    return 1;
}
