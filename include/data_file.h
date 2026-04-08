#pragma once

#include <cstdint>
#include <fstream>
#include <shared_mutex>
#include <string>

#include "log_record.h"
#include "status.h"

/// @brief DataFile 封装单个 segment 文件(data_<id>.log)的底层读写与持久化操作。
///
/// 该类负责：
/// - 追加写入逻辑记录（append-only）。
/// - 按偏移随机读取单条记录并做格式/CRC 校验。
/// - 提供 Sync/Truncate 等崩溃恢复相关能力。
class DataFile {
public:
    /// @brief 构造 DataFile 对象（仅保存元数据，不打开文件）。
    DataFile(uint32_t file_id, std::string file_path);

    /// @brief 析构函数，会尝试安全关闭底层文件句柄。
    ~DataFile();

    /// @brief 打开文件。
    /// @param writable 为 true 时以读写模式打开/创建；否则以只读模式打开。
    Status Open(bool writable);

    /// @brief 关闭文件并释放底层句柄。
    Status Close();

    /// @brief 追加一条逻辑记录到文件末尾。
    /// @param record 逻辑记录。
    /// @param offset [out] 返回记录起始偏移。
    /// @param written_size [out] 返回写入总字节数。
    Status Append(const LogRecord& record, uint64_t* offset, uint32_t* written_size);

    /// @brief 从指定偏移读取一条完整记录。
    /// @param offset 记录起始偏移。
    /// @param record [out] 解码后的逻辑记录。
    /// @param record_size [out] 记录总字节数。
    Status Read(uint64_t offset, LogRecord* record, uint32_t* record_size);

    /// @brief 将文件数据刷盘（flush + fsync/fdatasync）。
    Status Sync();

    /// @brief 获取当前文件大小。
    uint64_t Size();

    /// @brief 将文件截断到指定大小，常用于崩溃恢复时修剪坏尾。
    Status Truncate(uint64_t size);

    /// @brief 获取当前文件 ID。
    uint32_t FileId() const { return file_id_; }

    /// @brief 获取文件路径。
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
    bool need_dir_sync_ = false;
    int sync_fd_ = -1;
    mutable std::shared_mutex file_mutex_;
};
