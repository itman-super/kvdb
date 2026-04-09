#pragma once

#include <cstdint>
#include <fstream>
#include <shared_mutex>
#include <string>

#include "log_record.h"
#include "status.h"

class DataFile {
public:
    DataFile(uint32_t file_id, std::string file_path);
    ~DataFile();

    Status Open(bool writable);
    Status Close();

    Status Append(const LogRecord& record, uint64_t* offset, uint32_t* written_size);
    Status Read(uint64_t offset, LogRecord* record, uint32_t* record_size);

    Status Sync();

    uint64_t Size();

    Status Truncate(uint64_t size);

    uint32_t FileId() const { return file_id_; }
    const std::string& Path() const { return file_path_; }

private:
    Status WriteBytes(const char* data, std::size_t len);
    Status ReadBytes(uint64_t offset, char* data, std::size_t len);
    std::string EncodeRecord(const LogRecord& record);
    Status DecodeRecord(const std::string& buf, LogRecord* record);

private:
    uint32_t file_id_;
    std::string file_path_;
    std::fstream file_;
    bool writable_ = false;
    bool need_dir_sync_ = false;
    int sync_fd_ = -1;
    int read_fd_ = -1;
    mutable std::shared_mutex file_mutex_;
};
