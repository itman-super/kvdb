#pragma once

#include <cstdint>
#include <fstream>
#include <shared_mutex>
#include <string>

#include "log_record.h"
#include "status.h"

/// @brief 单个数据段（segment）文件的读写抽象。
///
/// DataFile 负责将逻辑层的 LogRecord 与磁盘二进制记录互相转换，并提供：
///  - 追加写（Append）
///  - 随机读（Read）
///  - 数据同步（Sync）
///  - 崩溃恢复截断（Truncate）
///
/// 每个 DataFile 对应一个 data_<id>.log 文件，线程安全由 file_mutex_ 保护。
class DataFile {
public:
    /// @brief 构造 DataFile 对象，仅记录文件标识与路径，不会立即打开文件。
    /// @param file_id   段文件编号（对应 data_<file_id>.log）。
    /// @param file_path 段文件绝对或相对路径。
    DataFile(uint32_t file_id, std::string file_path);
    /// @brief 析构函数，会自动调用 Close() 释放资源。
    ~DataFile();

    /// @brief 打开数据文件。
    /// @param writable true 表示读写模式打开，false 表示只读模式打开。
    /// @return 成功返回 OK，失败返回 IOError。
    Status Open(bool writable);
    /// @brief 关闭文件流与底层文件描述符，可重复调用（幂等）。
    /// @return 成功返回 OK，失败返回 IOError。
    Status Close();

    /// @brief 将一条日志记录追加到文件末尾。
    /// @param record       待写入的逻辑记录。
    /// @param offset       [out] 写入起始偏移，可为 nullptr。
    /// @param written_size [out] 写入字节数，可为 nullptr。
    /// @return 成功返回 OK，失败返回 IOError。
    Status Append(const LogRecord& record, uint64_t* offset, uint32_t* written_size);
    /// @brief 从指定偏移读取完整记录，并完成格式与 CRC 校验。
    /// @param offset      记录起始偏移。
    /// @param record      [out] 解码后的记录，不可为 nullptr。
    /// @param record_size [out] 记录总字节数，可为 nullptr。
    /// @return 成功返回 OK；偏移越界返回 OutOfRange；校验失败返回 Corruption/ChecksumFailed。
    Status Read(uint64_t offset, LogRecord* record, uint32_t* record_size);

    /// @brief 将写入数据刷盘，提升持久性保证。
    /// @return 成功返回 OK，失败返回 IOError。
    Status Sync();

    /// @brief 获取当前文件大小（字节）。
    /// @return 文件大小，获取失败时返回 0。
    uint64_t Size();

    /// @brief 将文件截断到指定大小，常用于恢复时裁剪半条记录。
    /// @param size 截断后的目标大小（字节）。
    /// @return 成功返回 OK，失败返回 IOError。
    Status Truncate(uint64_t size);

    /// @brief 返回该数据文件的 file_id。
    uint32_t FileId() const { return file_id_; }
    /// @brief 返回该数据文件路径。
    const std::string& Path() const { return file_path_; }

private:
    /// @brief 写入原始字节数据到底层文件流。
    Status WriteBytes(const char* data, std::size_t len);
    /// @brief 从底层文件按偏移读取原始字节数据。
    Status ReadBytes(uint64_t offset, char* data, std::size_t len);
    /// @brief 将 LogRecord 编码为磁盘二进制格式。
    std::string EncodeRecord(const LogRecord& record);
    /// @brief 将二进制记录解码为 LogRecord，并校验合法性。
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
