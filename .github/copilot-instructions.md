# Copilot Instructions for AI Coding Agents

## 项目架构概览
- 本项目为数据库相关的 C++ 工程，采用模块化设计，主要分为 `core`（核心功能）、`utils`（工具类）、`test`（测试）、`config`（配置）等目录。
- 主要核心类包括：
  - `DBManager`：数据库管理主入口，负责数据库的初始化、连接、表操作等。
  - `DBInitializer`：数据库初始化相关逻辑。
  - `DynamicTable`：动态表结构管理。
  - `MappedFileOper`：文件映射操作。
- 工具类如 `logger`（日志记录）、`ZipUtils`（压缩解压）位于 `utils` 目录。

## 关键开发流程
- **构建**：使用 CMake 构建，入口为 `db/CMakeLists.txt`。构建产物在 `db/build/` 目录。
  - 常用命令：
    ```bash
    cd db/build
    cmake ..
    make
    ```
- **测试**：测试代码位于 `db/test/`，如 `test_dbmanager.cpp`、`TestMappedFileOper.cpp`。
  - 测试可通过构建后生成的可执行文件运行。
- **配置**：数据库表结构等配置文件位于 `db/config/table_config.json`。

## 项目约定与模式
- 头文件统一放在 `db/include/`，实现文件在 `db/src/`。
- 命名空间和类名采用驼峰式命名。
- 日志统一使用 `logger` 工具类，便于调试和问题定位。
- 所有核心功能类都在 `db/include/core/` 和 `db/src/core/` 下，便于查找和维护。
- 测试代码与核心代码分离，便于独立开发和回归测试。

## 依赖与集成
- 主要依赖 C++ 标准库，无第三方库集成痕迹（如有请补充）。
- 构建和测试流程完全本地化，无外部服务依赖。

## 典型代码模式示例
- 数据库管理入口：
  ```cpp
  DBManager dbm;
  dbm.init(...);
  dbm.createTable(...);
  ```
- 日志记录：
  ```cpp
  logger::info("启动数据库...");
  ```
- 配置读取：
  ```cpp
  // 读取 table_config.json
  ```

## 重要文件/目录参考
- `db/include/core/`：核心头文件
- `db/src/core/`：核心实现
- `db/utils/`：工具类
- `db/test/`：测试代码
- `db/config/`：配置文件
- `db/CMakeLists.txt`：构建入口

---
如有特殊约定或未覆盖的开发流程，请补充说明。建议 AI 代理优先遵循上述结构和流程，遇到不明确的模式时参考核心类和工具类实现。
