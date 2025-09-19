#ifndef ISCADA_DB_MAPPED_FILE_OPER_H_
#define ISCADA_DB_MAPPED_FILE_OPER_H_

#include <string>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <cstdint>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <arpa/inet.h>

namespace ISCADA {
namespace DB {

enum class OpenMode {
    ReadOnly,
    ReadWrite,
    Create
};

class MappedFileOper {
public:
    static constexpr size_t HEADER_SIZE = 1024;
    static constexpr size_t COPYRIGHT_OFFSET = 0;
    static constexpr size_t USED_SIZE_OFFSET = 256;
    static constexpr const char* COPYRIGHT_NOTICE = "ISCADA Database File v1.0";
    static constexpr int FIXED_CHECK_INTERVAL = 5000; // 5��

    MappedFileOper(double expandThreshold = 0.2, std::chrono::milliseconds checkInterval = std::chrono::milliseconds(FIXED_CHECK_INTERVAL))
        : fd_(-1), data_(nullptr), size_(0), used_size_(0), mode_(OpenMode::ReadOnly),
          expand_threshold_(expandThreshold), check_interval_(checkInterval),
          running_(false) {}

    ~MappedFileOper();

    bool open(const std::string& filename, OpenMode mode, size_t initial_size = 0);
    void close();
    void write(const void* data, size_t length);
    // Write at given logical offset (offset relative to data area, not mapping)
    void writeAt(const void* data, size_t offset, size_t length);
    void read(void* buffer, size_t offset, size_t length) const;
    // Ensure there is capacity to append 'needed' bytes. Returns true if enough
    // space (possibly after expanding), false otherwise.
    bool ensureCapacity(size_t needed);
    bool isOpen() const { return fd_ != -1 && data_ != nullptr; }
    size_t getSize() const { return size_; }
    size_t getFileSize() const { return size_; }
    size_t getUsedSize() const { return used_size_; }
    OpenMode getMode() const { return mode_; }

private:
    int fd_;
    void* data_;
    size_t size_;
    size_t used_size_;
    std::string filename_;
    OpenMode mode_;
    double expand_threshold_;
    std::chrono::milliseconds check_interval_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread watchdog_thread_;
    bool running_;

    void startWatchdog();
    void stopWatchdog();
    void expandIfNeeded(bool force);
};

}
}

#endif
