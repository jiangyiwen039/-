#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <QDateTime>
#include <QDebug>
#include <thread>
#include <functional>
#include <type_traits>

// 日志级别
enum LogLevel {
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR
};

// 日志流实现
class LogStream {
public:
    LogStream(LogLevel level, const char* file, int line) 
        : level_(level), file_(file), line_(line) {}

    ~LogStream() {
        QString levelStr;
        switch (level_) {
            case LOG_DEBUG: levelStr = "DEBUG"; break;
            case LOG_INFO:  levelStr = "INFO";  break;
            case LOG_WARN:  levelStr = "WARN";  break;
            case LOG_ERROR: levelStr = "ERROR"; break;
        }

        QString log = QString("[%1] %2 (%3:%4)")
            .arg(QDateTime::currentDateTime().toString("yyyy-MM-dd HH:mm:ss"))
            .arg(buffer_)
            .arg(file_)
            .arg(line_);

        switch (level_) {
            case LOG_DEBUG: qDebug() << log; break;
            case LOG_INFO:  qInfo() << log;  break;
            case LOG_WARN:  qWarning() << log; break;
            case LOG_ERROR: qCritical() << log; break;
        }
    }

    // Only enable generic << for arithmetic types to avoid invalid conversions
    template<typename T, typename std::enable_if<std::is_arithmetic<T>::value, int>::type = 0>
    LogStream& operator<<(const T& value) {
        buffer_ += QString::number(value);
        return *this;
    }

    // Overload for std::thread::id (convert to numeric hash)
    LogStream& operator<<(const std::thread::id& id) {
        size_t h = std::hash<std::thread::id>{}(id);
        buffer_ += QString::number(static_cast<qulonglong>(h));
        return *this;
    }

    // Overload for char arrays (e.g., fixed-size buffers)
    template<size_t N>
    LogStream& operator<<(const char (&value)[N]) {
        buffer_ += QString(value);
        return *this;
    }

    LogStream& operator<<(const QString& value) {
        buffer_ += value;
        return *this;
    }

    LogStream& operator<<(const char* value) {
        buffer_ += QString(value);
        return *this;
    }

private:
    LogLevel level_;
    const char* file_;
    int line_;
    QString buffer_;
};

// 日志宏（自动获取文件名和行号）
#define LOG_DEBUG() LogStream(LOG_DEBUG, __FILE__, __LINE__)
#define LOG_INFO()  LogStream(LOG_INFO,  __FILE__, __LINE__)
#define LOG_WARN()  LogStream(LOG_WARN,  __FILE__, __LINE__)
#define LOG_ERROR() LogStream(LOG_ERROR, __FILE__, __LINE__)

#endif // LOGGER_H
