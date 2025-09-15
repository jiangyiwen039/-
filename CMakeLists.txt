cmake_minimum_required(VERSION 3.16)
project(ISCADA_DB)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")  # 增加警告与优化

# 配置Qt5
find_package(Qt5 COMPONENTS Core Widgets Test REQUIRED)

# 配置OpenSSL（对称加密依赖）
find_package(OpenSSL REQUIRED)
if(OPENSSL_FOUND)
    include_directories(${OPENSSL_INCLUDE_DIR})
else()
    message(FATAL_ERROR "OpenSSL not found")
endif()

# 手动配置QuaZip5
set(QUAZIP5_INCLUDE_DIRS "/usr/include/quazip5")
set(QUAZIP5_LIBRARY_DIRS "/usr/lib/x86_64-linux-gnu")
set(QUAZIP5_LIBRARIES "quazip5")

# 包含头文件目录
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/core
    ${CMAKE_CURRENT_SOURCE_DIR}/include/utils
    ${QUAZIP5_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
)

# 链接库目录
link_directories(
    ${QUAZIP5_LIBRARY_DIRS}
)

# 收集源文件
file(GLOB_RECURSE CORE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/src/core/MappedFileOper.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/core/DynamicTable.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/core/DBManager.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/src/core/DBInitializer.cpp"
)

file(GLOB_RECURSE UTILS_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/src/utils/ZipUtils.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/utils/logger.cpp"  # 新增日志实现
)

# 定义配置文件路径
set(CONFIG_FILE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/config/table_config.json")

# 构建时将配置文件复制到构建目录（与可执行文件同目录）
add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/table_config.json"
    COMMAND ${CMAKE_COMMAND} -E copy "${CONFIG_FILE_PATH}" "${CMAKE_CURRENT_BINARY_DIR}/table_config.json"
    DEPENDS "${CONFIG_FILE_PATH}"
    COMMENT "Copying table config file to build directory"
)

# 将复制操作关联到主目标，确保构建时执行
add_custom_target(
    copy_config_files ALL
    DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/table_config.json"
)

# 如果需要安装，将配置文件安装到系统目录（可选）
install(
    FILES "${CONFIG_FILE_PATH}"
    DESTINATION "${CMAKE_INSTALL_PREFIX}/bin/config"  # 与可执行文件同目录下的config子目录
)

# 主程序
add_executable(${PROJECT_NAME} 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
    ${CORE_SOURCES} 
    ${UTILS_SOURCES}
)

target_link_libraries(${PROJECT_NAME} 
    Qt5::Core 
    Qt5::Widgets 
    ${QUAZIP5_LIBRARIES}
    ${OPENSSL_LIBRARIES}  # 链接OpenSSL
    pthread
)

# 启用Qt元对象编译
set_target_properties(${PROJECT_NAME} PROPERTIES
    AUTOMOC ON
    AUTOUIC ON
    AUTORCC ON
)

# 测试程序 - TestMappedFileOper
add_executable(TestMappedFileOper
    test/TestMappedFileOper.cpp
    ${CORE_SOURCES}
    ${UTILS_SOURCES}
)

target_link_libraries(TestMappedFileOper
    Qt5::Core
    ${QUAZIP5_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    pthread
)

# 测试程序 - test_dbmanager
add_executable(test_dbmanager
    test/test_dbmanager.cpp
    ${CORE_SOURCES}
    ${UTILS_SOURCES}
)

target_link_libraries(test_dbmanager
    Qt5::Core
    Qt5::Widgets
    ${QUAZIP5_LIBRARIES}
    Qt5::Test
    ${OPENSSL_LIBRARIES}
    pthread
)

# 启用Qt元对象编译 for 测试程序
set_target_properties(TestMappedFileOper test_dbmanager PROPERTIES
    AUTOMOC ON
    AUTOUIC ON
    AUTORCC ON
)

# 安装配置
install(TARGETS ${PROJECT_NAME} TestMappedFileOper test_dbmanager
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
