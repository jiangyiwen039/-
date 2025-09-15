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
        std::cerr << "����ȱ���ļ�·������" << std::endl;
        return false;
    }
    try {
        bool flag = mfo.open(args[0], OpenMode::Create, 1024 * 1024);
        std::cout << (flag ? "��ʼ���ɹ�" : "��ʼ��ʧ��") << std::endl;
        return flag;
    } catch (const std::exception& e) {
        std::cerr << "��ʼ��ʧ�ܣ�" << e.what() << std::endl;
        return false;
    }
}

bool writeMappedFile(std::vector<std::string> args) {
    if (args.empty()) {
        std::cerr << "����ȱ���ļ�·������" << std::endl;
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
        std::cout << "д�����" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "д��ʧ�ܣ�" << e.what() << std::endl;
        return false;
    }
}

bool readMappedFile(std::vector<std::string> args) {
    if (args.size() < 2) {
        std::cerr << "����ȱ���ļ�·����ƫ��������" << std::endl;
        return false;
    }
    try {
        long offset = std::stol(args[1]);
        char buffer[64] = {0};
        if (!mfo.isOpen()) {
            mfo.open(args[0], OpenMode::ReadOnly, -1);
        }
        mfo.read(buffer, offset, 63);
        std::cout << "��ȡ����: " << buffer << std::endl;
        mfo.close();
        return true;
    } catch (const std::invalid_argument& e) {
        std::cerr << "ƫ������������" << e.what() << std::endl;
        return false;
    } catch (const std::out_of_range& e) {
        std::cerr << "��ȡ��Χ����" << e.what() << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "��ȡʧ�ܣ�" << e.what() << std::endl;
        return false;
    }
}

bool testDynamicTable(std::vector<std::string> args) {
    if (args.empty()) {
        std::cerr << "����ȱ���ļ�·������" << std::endl;
        return false;
    }
    try {
        DynamicTable table;
        std::vector<FieldDef> fields = {
            {TYPE_INT, sizeof(int), "id"},
            {TYPE_STRING, 32, "name"}
        };
        if (!table.initTable(QString::fromStdString(args[0]), fields)) {
            std::cerr << "��̬���ʼ��ʧ��" << std::endl;
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
        std::cout << (success ? "��̬��д��ɹ�" : "��̬��д��ʧ��") << std::endl;
        return success;
    } catch (const std::exception& e) {
        std::cerr << "��̬�����ʧ�ܣ�" << e.what() << std::endl;
        return false;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "�÷�: " << argv[0] << " [����] [����...]" << std::endl;
        std::cerr << "�����б�:" << std::endl;
        std::cerr << "  -c, --create   �����ڴ�ӳ���ļ�" << std::endl;
        std::cerr << "  -w, --write    д���ļ�" << std::endl;
        std::cerr << "  -r, --read     ��ȡ�ļ���������ƫ������" << std::endl;
        std::cerr << "  -t, --table    ���Զ�̬��" << std::endl;
        return 1;
    }

    std::vector<Command> commands = {
        {{"-c", "--create"}, "�����ڴ�ӳ���ļ�", initMappedFile, 1},
        {{"-w", "--write"}, "д���ļ�", writeMappedFile, 1},
        {{"-r", "--read"}, "��ȡ�ļ�", readMappedFile, 2},
        {{"-t", "--table"}, "���Զ�̬��", testDynamicTable, 1}
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
                    std::cerr << "���󣺲�����������" << std::endl;
                    return 1;
                }
                cmd.action(args);
                return 0;
            }
        }
    }

    std::cerr << "����δ֪����" << std::endl;
    return 1;
}
