#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "log_record.h"
#include "status.h"

// DataFile 负责单个日志文件的读写。
// 第一版只维护一个 active data file，因此它承担：
// 1. 追加写日志记录
// 2. 按 offset 读取日志记录
// 3. 文件 flush/sync
class DataFile {
public:
    DataFile(uint32_t file_id, std::string file_path);
    ~DataFile();

    // 打开文件
    // writable=true 代表需要同时支持写入
    Status Open(bool writable);

    // 关闭文件
    Status Close();

    // 追加一条逻辑记录到文件末尾
    // offset: 返回本条记录起始偏移
    // written_size: 返回写入字节数
    Status Append(const LogRecord& record, uint64_t* offset, uint32_t* written_size);

    // 从指定偏移读取一条完整记录
    Status Read(uint64_t offset, LogRecord* record, uint32_t* record_size);

    // 刷盘
    // 第一版仅 flush 到文件缓冲区，不保证真正 fsync 到磁盘设备
    Status Sync();

    // 获取文件大小
    uint64_t Size();

    uint32_t FileId() const { return file_id_; }
    const std::string& Path() const { return file_path_; }

private:
    // 底层字节写入
    Status WriteBytes(const char* data, std::size_t len);

    // 底层字节读取
    Status ReadBytes(uint64_t offset, char* data, std::size_t len);

    // 将逻辑记录编码为二进制格式
    std::string EncodeRecord(const LogRecord& record);

    // 将二进制数据解码回逻辑记录
    Status DecodeRecord(const std::string& buf, LogRecord* record);

private:
    uint32_t file_id_;
    std::string file_path_;
    std::fstream file_;
    bool writable_ = false;
};