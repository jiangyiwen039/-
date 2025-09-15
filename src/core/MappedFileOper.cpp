#include "MappedFileOper.h"
#include <system_error>
#include <cstring>
#include <stdexcept>
#include <iostream>

namespace ISCADA {
namespace DB {

MappedFileOper::~MappedFileOper() {
    close();
}

void MappedFileOper::write(const void* data, size_t length) {
    if (mode_ == OpenMode::ReadOnly) {
        throw std::runtime_error("File opened in read-only mode");
    }

    if (!isOpen()) {
        throw std::runtime_error("File not open");
    }

    size_t actual_offset = used_size_;
    if (actual_offset + length > size_) {
        throw std::out_of_range("Write operation exceeds file bounds");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    std::memcpy(static_cast<char*>(data_) + actual_offset, data, length);
    used_size_ += length;
    
    double free_ratio = 1.0 - (static_cast<double>(used_size_) / size_);
    if (free_ratio < expand_threshold_) {
        cv_.notify_one();
    }
}

void MappedFileOper::read(void* buffer, size_t offset, size_t length) const {
    size_t actual_offset = offset + HEADER_SIZE;
    if (!isOpen()) {
        throw std::runtime_error("File not open");
    }
    if (actual_offset + length > size_) {
        throw std::out_of_range("Read operation exceeds file bounds");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    std::memcpy(buffer, static_cast<const char*>(data_) + actual_offset, length);
}

bool MappedFileOper::open(const std::string& filename, OpenMode mode, size_t initial_size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (isOpen()) {
        throw std::runtime_error("File already open");
    }

    int flags = 0;
    int prot = 0;

    switch (mode) {
        case OpenMode::ReadOnly: flags = O_RDONLY; prot = PROT_READ; break;
        case OpenMode::ReadWrite: flags = O_RDWR; prot = PROT_READ | PROT_WRITE; break;
        case OpenMode::Create: flags = O_RDWR | O_CREAT | O_TRUNC; prot = PROT_READ | PROT_WRITE; break;
    }

    fd_ = ::open(filename.c_str(), flags, 0666);
    if (fd_ == -1) throw std::runtime_error("Failed to open file");

    if (mode == OpenMode::Create) {
        if (initial_size == 0) throw std::runtime_error("Initial size required for create mode");
        size_ = initial_size;
        if (size_ < HEADER_SIZE) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("Initial size too small");
        }
        if (ftruncate(fd_, size_) == -1) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("Failed to set file size");
        }
    } else {
        struct stat st;
        if (fstat(fd_, &st) == -1) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("Failed to get file size");
        }
        size_ = st.st_size;
    }

    data_ = mmap(nullptr, size_, prot, MAP_SHARED, fd_, 0);
    if (data_ == MAP_FAILED) {
        data_ = nullptr;
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Failed to create memory mapping");
    }

    filename_ = filename;
    mode_ = mode;

    if (mode == OpenMode::Create) {
        std::memcpy(static_cast<char*>(data_) + COPYRIGHT_OFFSET, 
                   COPYRIGHT_NOTICE, std::strlen(COPYRIGHT_NOTICE));
        used_size_ = HEADER_SIZE;
        uint64_t used_size_le = htole64(used_size_);
        std::memcpy(static_cast<char*>(data_) + USED_SIZE_OFFSET, &used_size_le, sizeof(used_size_le));
        close();
    } else {
        uint64_t used_size_le;
        std::memcpy(&used_size_le, static_cast<char*>(data_) + USED_SIZE_OFFSET, sizeof(used_size_le));
        used_size_ = le64toh(used_size_le);
        
        if (std::strncmp(static_cast<const char*>(data_) + COPYRIGHT_OFFSET,
                        COPYRIGHT_NOTICE, std::strlen(COPYRIGHT_NOTICE)) != 0) {
            ::munmap(data_, size_);
            ::close(fd_);
            fd_ = -1;
            data_ = nullptr;
            throw std::runtime_error("Invalid file format");
        }
        
        if (mode != OpenMode::ReadOnly) {
            startWatchdog();
        }
    }
    return true;
}

void MappedFileOper::close() {
    if (mode_ == OpenMode::ReadWrite) stopWatchdog();
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (isOpen()) {
        if (mode_ != OpenMode::ReadOnly) {
            uint64_t used_size_le = htole64(used_size_);
            std::memcpy(static_cast<char*>(data_) + USED_SIZE_OFFSET, &used_size_le, sizeof(used_size_le));
            ::msync(data_, size_, MS_SYNC);
        }
        
        if (data_ != nullptr) {
            munmap(data_, size_);
            data_ = nullptr;
        }
        if (fd_ != -1) {
            ::close(fd_);
            fd_ = -1;
        }
        size_ = 0;
        used_size_ = 0;
        filename_.clear();
    }
}

void MappedFileOper::startWatchdog() {
    if (running_) return;
    running_ = true;
    watchdog_thread_ = std::thread([this]() {
        while (running_) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, check_interval_, [this]() { return !running_; });
            expandIfNeeded(false);
        }
    });
}

void MappedFileOper::stopWatchdog() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) return;
        running_ = false;
    }
    cv_.notify_all();  // 唤醒等待的线程
    if (watchdog_thread_.joinable()) {
        // 正确的判断方式：获取线程的id进行比较
        if (watchdog_thread_.get_id() != std::this_thread::get_id()) {
            watchdog_thread_.join();
        } else {
            // 不能连接当前线程，选择分离
            watchdog_thread_.detach();
        }
    }
}

void MappedFileOper::expandIfNeeded(bool force) {
    if (!isOpen() || mode_ == OpenMode::ReadOnly) return;
    
    double free_ratio = 1.0 - (static_cast<double>(used_size_) / size_);
    if (force || free_ratio < expand_threshold_) {
        size_t new_size = std::max(static_cast<size_t>(size_ * 1.25), size_ + (1 << 20));
        if (ftruncate(fd_, new_size) == -1) {
            std::cerr << "Failed to expand file" << std::endl;
            return;
        }
        
        void* new_mapping = mremap(data_, size_, new_size, MREMAP_MAYMOVE);
        if (new_mapping == MAP_FAILED) {
            std::cerr << "Failed to remap file" << std::endl;
            ftruncate(fd_, size_);
            return;
        }
        
        data_ = new_mapping;
        size_ = new_size;
        std::cout << "Expanded file to " << new_size << " bytes" << std::endl;
    }
}

}
}
