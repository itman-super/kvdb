// src/data_file.cpp
//
// DataFile 实现文件。
// 每个 DataFile 对应磁盘上一个 segment 文件（data_<id>.log）。
// 所有读写均使用 std::fstream 完成，sync 则通过单独的原生文件描述符
// (sync_fd_) 调用 fsync/fdatasync 实现，以避免 C++ 流层无法直接 sync 的问题。

#include "data_file.h"

#include <cstring>
#include <filesystem>
#include <limits>  // numeric_limits
#include <mutex>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {

// 新的 magic，可以和旧版本区分。
// 如果你不想改 magic，也可以保持原值，但旧数据文件将无法兼容。
constexpr uint32_t kMagic = 0x4B564443;  // "KVDC"

// 记录头布局：
// magic(4) + type(1) + timestamp(8) + key_size(4) + value_size(4) + crc(4)
constexpr std::size_t kHeaderSize =
    sizeof(uint32_t) +   // magic
    sizeof(uint8_t) +    // type
    sizeof(uint64_t) +   // timestamp
    sizeof(uint32_t) +   // key_size
    sizeof(uint32_t) +   // value_size
    sizeof(uint32_t);    // crc

/// @brief 将固定大小的 POD 值以原始字节追加到字符串末尾（小端序存储）。
/// @tparam T  任意平凡可复制类型（uint32_t、uint64_t 等）。
/// @param out  目标字符串，字节追加到其末尾。
/// @param value 要追加的值。
template <typename T>
void AppendFixed(std::string* out, const T& value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(T));
}

/// @brief 从缓冲区当前位置读取一个固定大小的 POD 值，并将 pos 向后推进。
/// @tparam T  目标类型。
/// @param buf  源字节缓冲。
/// @param pos  当前读取位置（in/out），读取成功后会前移 sizeof(T) 字节。
/// @param value 读出的值写入此处。
/// @return 若剩余字节不足则返回 false，否则返回 true。
template <typename T>
bool ReadFixed(const std::string& buf, std::size_t* pos, T* value) {
    if (*pos + sizeof(T) > buf.size()) {
        return false;
    }
    std::memcpy(value, buf.data() + *pos, sizeof(T));
    *pos += sizeof(T);
    return true;
}

/// @brief 增量更新 CRC-32 校验值（Castagnoli 多项式 0xEDB88320，ISO 3309 标准）。
///
/// 采用逐位运算实现，无需查找表，避免静态初始化开销。
/// 调用前将 crc 初始化为 0，函数内部会自动取反处理（~crc 进入，~crc 返回）。
///
/// @param crc  当前累积 CRC 值（初始调用传 0）。
/// @param data 待校验数据指针。
/// @param len  数据长度（字节）。
/// @return 更新后的 CRC-32 值。
// CRC32 实现（无外部依赖）
uint32_t CRC32Update(uint32_t crc, const char* data, std::size_t len) {
    crc = ~crc;
    for (std::size_t i = 0; i < len; ++i) {
        crc ^= static_cast<uint8_t>(data[i]);
        for (int j = 0; j < 8; ++j) {
            if (crc & 1U) {
                crc = (crc >> 1U) ^ 0xEDB88320U;
            } else {
                crc >>= 1U;
            }
        }
    }
    return ~crc;
}

/// @brief 对单个固定大小 POD 值进行 CRC-32 增量计算的便捷重载。
/// @tparam T  POD 类型。
/// @param crc  当前累积 CRC 值。
/// @param value 待纳入校验的值。
/// @return 更新后的 CRC-32 值。
template <typename T>
uint32_t CRC32UpdateFixed(uint32_t crc, const T& value) {
    return CRC32Update(crc, reinterpret_cast<const char*>(&value), sizeof(T));
}

/// @brief 计算一条完整日志记录的 CRC-32 校验值。
///
/// 覆盖范围：type | timestamp | key_size | value_size | key bytes | value bytes。
/// magic 字段不纳入 CRC，以便在磁盘损坏时通过 magic 和 CRC 分别定位不同类型的错误。
///
/// @param type        记录类型（kPut / kDelete）。
/// @param timestamp   写入时间戳。
/// @param key_size    key 字节数。
/// @param value_size  value 字节数。
/// @param key_data    key 数据指针（key_size == 0 时可为 nullptr）。
/// @param value_data  value 数据指针（value_size == 0 时可为 nullptr）。
/// @return 计算得到的 CRC-32 值。
// 计算一条记录的 CRC：
// 覆盖 type | timestamp | key_size | value_size | key | value
uint32_t ComputeRecordCRC(uint8_t type,
                          uint64_t timestamp,
                          uint32_t key_size,
                          uint32_t value_size,
                          const char* key_data,
                          const char* value_data) {
    uint32_t crc = 0;
    crc = CRC32UpdateFixed(crc, type);
    crc = CRC32UpdateFixed(crc, timestamp);
    crc = CRC32UpdateFixed(crc, key_size);
    crc = CRC32UpdateFixed(crc, value_size);

    if (key_size > 0) {
        crc = CRC32Update(crc, key_data, key_size);
    }
    if (value_size > 0) {
        crc = CRC32Update(crc, value_data, value_size);
    }
    return crc;
}

#ifndef _WIN32
/// @brief 在 POSIX 平台上对文件所在目录执行 fsync，确保目录项（文件名）持久化。
///
/// 新建文件后首次写入时需要 sync 目录，否则在宕机重启后目录项可能尚未落盘，
/// 导致文件"消失"。只在 need_dir_sync_ == true 时调用一次，之后置为 false。
///
/// @param file_path 数据文件的完整路径，函数会从中提取父目录。
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status SyncDirectoryIfNeeded(const std::string& file_path) {
    std::error_code ec;
    const std::filesystem::path dir_path = std::filesystem::path(file_path).parent_path();
    if (dir_path.empty()) {
        return Status::OK();
    }

    int flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif

    int dir_fd = ::open(dir_path.c_str(), flags);
    if (dir_fd < 0) {
        return Status::IOError("open dir for sync failed");
    }

    if (::fsync(dir_fd) != 0) {
        ::close(dir_fd);
        return Status::IOError("fsync dir failed");
    }

    ::close(dir_fd);
    return Status::OK();
}
#endif

}  // namespace

/// @brief 构造 DataFile 对象，仅记录文件 ID 和路径，不打开文件。
/// 调用方必须显式调用 Open() 才能进行实际读写。
DataFile::DataFile(uint32_t file_id, std::string file_path)
    : file_id_(file_id), file_path_(std::move(file_path)) {}

/// @brief 析构时自动关闭文件句柄，释放 fstream 和 sync_fd_ 资源。
DataFile::~DataFile() {
    Close();
}

/// @brief 打开底层文件，并根据 writable 参数决定是否同时支持写入。
///
/// 打开策略：
///  1. 若文件已打开则直接返回 OK（幂等）。
///  2. 以 binary + in 模式打开；若 writable == true，追加 out 模式。
///  3. 若以可写模式打开时文件不存在，先创建空文件再重新打开（两步法）。
///     此时标记 need_dir_sync_ = true，下次 Sync() 时会对目录执行 fsync。
///  4. 成功后为可写文件额外打开一个原生 fd（sync_fd_），用于后续 fsync/fdatasync。
///
/// @param writable true 表示需要读写，false 表示只读。
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status DataFile::Open(bool writable) {
    std::unique_lock<std::shared_mutex> lock(file_mutex_);
    writable_ = writable;

    if (file_.is_open()) {
        return Status::OK();
    }

    std::ios::openmode mode = std::ios::binary | std::ios::in;
    if (writable_) {
        mode |= std::ios::out;
    }

    file_.open(file_path_, mode);

    if (!file_.is_open() && writable_) {
        // 可写模式下若文件不存在，先尝试创建空文件再以读写模式重新打开。
        std::ofstream create(file_path_, std::ios::binary | std::ios::out);
        if (!create.is_open()) {
            return Status::IOError("failed to create file: " + file_path_);
        }
        create.close();
        need_dir_sync_ = true;
        file_.open(file_path_, mode);
    }

    if (!file_.is_open()) {
        return Status::IOError("failed to open file: " + file_path_);
    }

    if (writable_) {
#ifdef _WIN32
        sync_fd_ = _open(file_path_.c_str(), _O_BINARY | _O_RDWR);
#else
        sync_fd_ = ::open(file_path_.c_str(), O_RDWR);
#endif
        if (sync_fd_ < 0) {
            file_.close();
            return Status::IOError("failed to open sync fd: " + file_path_);
        }
    }

#ifdef _WIN32
    read_fd_ = _open(file_path_.c_str(), _O_BINARY | _O_RDONLY);
#else
    read_fd_ = ::open(file_path_.c_str(), O_RDONLY);
#endif
    if (read_fd_ < 0) {
        if (sync_fd_ >= 0) {
#ifdef _WIN32
            _close(sync_fd_);
#else
            ::close(sync_fd_);
#endif
            sync_fd_ = -1;
        }
        file_.close();
        return Status::IOError("failed to open read fd: " + file_path_);
    }

    return Status::OK();
}

/// @brief 关闭文件句柄，先关闭 sync_fd_（原生 fd），再 flush 并关闭 fstream。
///
/// 可以安全地多次调用（幂等）：若文件已关闭则静默返回 OK。
/// 析构函数会自动调用此方法，因此不必担心泄漏。
///
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status DataFile::Close() {
    std::unique_lock<std::shared_mutex> lock(file_mutex_);
#ifdef _WIN32
    if (read_fd_ >= 0) {
        _close(read_fd_);
        read_fd_ = -1;
    }
    if (sync_fd_ >= 0) {
        _close(sync_fd_);
        sync_fd_ = -1;
    }
#else
    if (read_fd_ >= 0) {
        ::close(read_fd_);
        read_fd_ = -1;
    }
    if (sync_fd_ >= 0) {
        ::close(sync_fd_);
        sync_fd_ = -1;
    }
#endif

    if (file_.is_open()) {
        file_.flush();
        file_.close();
    }
    return Status::OK();
}

/// @brief 将用户态缓冲区刷入内核，并调用 fsync/fdatasync 确保数据持久化到磁盘。
///
/// 步骤：
///  1. 调用 fstream::flush() 将 C++ 流缓冲写入操作系统内核缓冲区。
///  2. 通过 sync_fd_（原生 fd）调用平台特定的 sync 系统调用：
///     - Windows：_commit()
///     - macOS：fsync()（fdatasync 在 macOS 上不完整）
///     - 其他 POSIX：fdatasync()（不同步 metadata，性能更好）
///  3. 若为新建文件（need_dir_sync_ == true），额外对目录执行 fsync，
///     确保目录项持久化后再清除该标志。
///
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status DataFile::Sync() {
    std::unique_lock<std::shared_mutex> lock(file_mutex_);
    if (!file_.is_open()) {
        return Status::IOError("file not open");
    }

    file_.flush();
    if (!file_) {
        return Status::IOError("flush failed");
    }

#ifdef _WIN32
    if (sync_fd_ < 0) {
        return Status::IOError("sync fd not open");
    }

    if (_commit(sync_fd_) != 0) {
        return Status::IOError("commit failed");
    }
    need_dir_sync_ = false;
#else
    if (sync_fd_ < 0) {
        return Status::IOError("sync fd not open");
    }

#if defined(__APPLE__)
    if (::fsync(sync_fd_) != 0) {
        return Status::IOError("fsync failed");
    }
#else
    if (::fdatasync(sync_fd_) != 0) {
        return Status::IOError("fdatasync failed");
    }
#endif

    if (need_dir_sync_) {
        Status s = SyncDirectoryIfNeeded(file_path_);
        if (!s.ok()) {
            return s;
        }
        need_dir_sync_ = false;
    }
#endif

    return Status::OK();
}

/// @brief 返回文件当前大小（字节数）。
///
/// 通过 seekg 到文件末尾再 tellg 实现，不依赖 stat 系统调用，
/// 因此对跨平台（Windows/POSIX）均适用。
/// 若文件未打开或 seek 失败则返回 0，调用方应将 0 视为"无法获取尺寸"。
///
/// @return 文件字节数，失败返回 0。
uint64_t DataFile::Size() {
    std::unique_lock<std::shared_mutex> lock(file_mutex_);
    if (!file_.is_open()) {
        return 0;
    }

    file_.clear();
    file_.seekg(0, std::ios::end);
    if (!file_) {
        // seek 失败时返回 0，调用方会把它当作“无法获取尺寸”处理。
        return 0;
    }

    std::streamoff pos = file_.tellg();
    if (pos < 0) {
        return 0;
    }
    return static_cast<uint64_t>(pos);
}

/// @brief 向文件当前写指针位置写入 len 字节的原始数据。
///
/// 这是所有写操作的最终底层调用，fstream 内部维护写指针位置，
/// 上层 Append() 在调用前会先 seekp 到文件末尾。
///
/// @param data 源数据指针。
/// @param len  写入字节数。
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status DataFile::WriteBytes(const char* data, std::size_t len) {
    if (!file_.is_open()) {
        return Status::IOError("file not open");
    }

    file_.write(data, static_cast<std::streamsize>(len));
    if (!file_) {
        return Status::IOError("write failed");
    }
    return Status::OK();
}

/// @brief 从指定偏移量读取 len 字节的原始数据。
///
/// 读取前会先进行边界校验（offset + len <= file_size），避免在 EOF 处
/// 才发现读取失败的情况，使错误更早、更清晰地暴露为 OutOfRange 而非 IOError。
/// seekg 后若流状态异常（如流已损坏），也会返回 IOError。
///
/// @param offset 文件内起始偏移（字节）。
/// @param data   目标缓冲区指针，调用方负责保证至少 len 字节可写。
/// @param len    需要读取的字节数。
/// @return 成功返回 Status::OK()，offset 越界返回 OutOfRange，其他失败返回 IOError。
Status DataFile::ReadBytes(uint64_t offset, char* data, std::size_t len) {
    std::shared_lock<std::shared_mutex> lock(file_mutex_);
    if (read_fd_ < 0) {
        return Status::IOError("file not open");
    }

#ifdef _WIN32
    const __int64 file_size = _lseeki64(read_fd_, 0, SEEK_END);
    if (file_size < 0) {
        return Status::IOError("seek end failed");
    }
#else
    struct stat st;
    if (fstat(read_fd_, &st) != 0) {
        return Status::IOError("fstat failed");
    }
    const uint64_t file_size = static_cast<uint64_t>(st.st_size);
#endif

    if (offset > file_size) {
        return Status::OutOfRange("offset out of range: " + std::to_string(offset) +
                                  ", file_size: " + std::to_string(file_size));
    }
    if (len > file_size - offset) {
        return Status::OutOfRange("read out of range, offset: " + std::to_string(offset) +
                                  ", len: " + std::to_string(len) +
                                  ", file_size: " + std::to_string(file_size));
    }

#ifdef _WIN32
    int got = _pread(read_fd_, data, static_cast<unsigned int>(len), static_cast<__int64>(offset));
    if (got != static_cast<int>(len)) {
        return Status::IOError("pread failed");
    }
#else
    ssize_t got = ::pread(read_fd_, data, len, static_cast<off_t>(offset));
    if (got != static_cast<ssize_t>(len)) {
        return Status::IOError("pread failed");
    }
#endif

    return Status::OK();
}

/// @brief 将逻辑 LogRecord 编码为二进制字节序列（用于追加写入磁盘）。
///
/// 编码格式（固定头 + 可变体）：
///   [magic: 4B][type: 1B][timestamp: 8B][key_size: 4B][value_size: 4B][crc: 4B]
///   [key: key_size B][value: value_size B]
///
/// CRC 覆盖 type、timestamp、key_size、value_size 以及 key/value 内容，
/// 不覆盖 magic（magic 损坏时通过魔数校验单独检测）。
///
/// @param record 要编码的逻辑记录。
/// @return 编码后的二进制字节串。
std::string DataFile::EncodeRecord(const LogRecord& record) {
    std::string out;
    out.reserve(kHeaderSize + record.key.size() + record.value.size());

    const uint32_t magic = kMagic;
    const uint8_t type = static_cast<uint8_t>(record.type);
    const uint64_t timestamp = record.timestamp;
    const uint32_t key_size = static_cast<uint32_t>(record.key.size());
    const uint32_t value_size = static_cast<uint32_t>(record.value.size());

    const uint32_t crc = ComputeRecordCRC(
        type,
        timestamp,
        key_size,
        value_size,
        record.key.data(),
        record.value.data()
    );

    AppendFixed(&out, magic);
    AppendFixed(&out, type);
    AppendFixed(&out, timestamp);
    AppendFixed(&out, key_size);
    AppendFixed(&out, value_size);
    AppendFixed(&out, crc);
    out.append(record.key);
    out.append(record.value);

    return out;
}

/// @brief 将二进制缓冲区解码为 LogRecord，并完成完整性验证。
///
/// 解码步骤：
///  1. 检查缓冲区长度至少为 kHeaderSize。
///  2. 依次读取各头部字段（magic、type、timestamp、key_size、value_size、crc）。
///  3. 验证 magic 值匹配 kMagic。
///  4. 验证 type 为合法值（kPut 或 kDelete），其他值视为格式损坏。
///  5. 验证缓冲区总长度等于 kHeaderSize + key_size + value_size。
///  6. 重新计算 CRC 并与存储值对比，不一致则返回 ChecksumFailed。
///
/// @param buf    完整的序列化记录字节串（头部 + payload）。
/// @param record 解码后的逻辑记录写入此处。
/// @return 成功返回 Status::OK()，格式损坏返回 Corruption，CRC 不符返回 ChecksumFailed。
Status DataFile::DecodeRecord(const std::string& buf, LogRecord* record) {
    if (buf.size() < kHeaderSize) {
        return Status::Corruption("record too small");
    }

    std::size_t pos = 0;
    uint32_t magic = 0;
    uint8_t type = 0;
    uint64_t timestamp = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    uint32_t stored_crc = 0;

    if (!ReadFixed(buf, &pos, &magic) ||
        !ReadFixed(buf, &pos, &type) ||
        !ReadFixed(buf, &pos, &timestamp) ||
        !ReadFixed(buf, &pos, &key_size) ||
        !ReadFixed(buf, &pos, &value_size) ||
        !ReadFixed(buf, &pos, &stored_crc)) {
        return Status::Corruption("bad record header");
    }

    if (magic != kMagic) {
        return Status::Corruption("bad magic");
    }

    // 当前版本仅接受 Put/Delete 两种记录类型，其他值视为格式损坏。
    if (type != static_cast<uint8_t>(RecordType::kPut) &&
        type != static_cast<uint8_t>(RecordType::kDelete)) {
        return Status::Corruption("bad record type");
    }

    if (buf.size() != kHeaderSize + key_size + value_size) {
        return Status::Corruption("record size mismatch");
    }

    const char* key_ptr = buf.data() + pos;
    const char* value_ptr = key_ptr + key_size;

    const uint32_t actual_crc = ComputeRecordCRC(
        type,
        timestamp,
        key_size,
        value_size,
        key_ptr,
        value_ptr
    );

    if (actual_crc != stored_crc) {
        return Status::ChecksumFailed("crc mismatch");
    }

    record->type = static_cast<RecordType>(type);
    record->timestamp = timestamp;
    record->key.assign(key_ptr, key_size);
    record->value.assign(value_ptr, value_size);

    return Status::OK();
}

/// @brief 将一条逻辑记录追加到文件末尾，并返回写入位置和大小。
///
/// 流程：
///  1. 调用 EncodeRecord() 将记录序列化为字节串。
///  2. seekp 到文件末尾，记录当前偏移（即本条记录起始位置）。
///  3. 调用 WriteBytes() 写入字节数据。
///  4. 将起始偏移和写入字节数通过输出参数返回给调用方，
///     调用方（AppendRecord）会据此更新内存索引。
///
/// @param record       要追加的逻辑记录。
/// @param offset       [out] 本条记录在文件中的起始偏移（字节），可为 nullptr。
/// @param written_size [out] 实际写入的字节数，可为 nullptr。
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status DataFile::Append(const LogRecord& record, uint64_t* offset, uint32_t* written_size) {
    std::unique_lock<std::shared_mutex> lock(file_mutex_);
    if (!file_.is_open()) {
        return Status::IOError("file not open");
    }

    std::string encoded = EncodeRecord(record);

    file_.clear();
    file_.seekp(0, std::ios::end);
    if (!file_) {
        return Status::IOError("seekp end failed");
    }

    std::streamoff pos = file_.tellp();
    if (pos < 0) {
        return Status::IOError("tellp failed");
    }

    Status s = WriteBytes(encoded.data(), encoded.size());
    if (!s.ok()) {
        return s;
    }
    file_.flush();
    if (!file_) {
        return Status::IOError("flush after append failed");
    }

    if (offset != nullptr) {
        *offset = static_cast<uint64_t>(pos);
    }
    if (written_size != nullptr) {
        *written_size = static_cast<uint32_t>(encoded.size());
    }

    return Status::OK();
}

/// @brief 从指定偏移读取一条完整的日志记录（包括头部与 payload）。
///
/// 采用两次读取策略以减少不必要的内存分配：
///  1. 先读取固定大小的头部（kHeaderSize 字节），解析出 key_size 和 value_size。
///  2. 再读取 payload（key + value），最终调用 DecodeRecord 做完整校验。
///
/// 防御性检查：
///  - key_size + value_size 可能整数溢出，使用 uint64_t 中间变量防范。
///  - total_size 超过 uint32_t 范围时返回 Corruption（单条记录不应如此巨大）。
///
/// @param offset      从此文件偏移开始读取（字节）。
/// @param record      [out] 解码后的逻辑记录写入此处，不得为 nullptr。
/// @param record_size [out] 本条记录占用的总字节数（头部 + payload），可为 nullptr。
/// @return 成功返回 Status::OK()；偏移越界返回 OutOfRange；格式损坏返回 Corruption；
///         CRC 不符返回 ChecksumFailed；其他 I/O 失败返回 IOError。
Status DataFile::Read(uint64_t offset, LogRecord* record, uint32_t* record_size) {
    if (record == nullptr) {
        return Status::InvalidArgument("record output is null");
    }

    char header[kHeaderSize];
    Status s = ReadBytes(offset, header, sizeof(header));
    if (!s.ok()) {
        return s;
    }

    std::string header_buf(header, sizeof(header));

    std::size_t pos = 0;
    uint32_t magic = 0;
    uint8_t type = 0;
    uint64_t timestamp = 0;
    uint32_t key_size = 0;
    uint32_t value_size = 0;
    uint32_t crc = 0;

    if (!ReadFixed(header_buf, &pos, &magic) ||
        !ReadFixed(header_buf, &pos, &type) ||
        !ReadFixed(header_buf, &pos, &timestamp) ||
        !ReadFixed(header_buf, &pos, &key_size) ||
        !ReadFixed(header_buf, &pos, &value_size) ||
        !ReadFixed(header_buf, &pos, &crc)) {
        return Status::Corruption("bad header");
    }

    if (magic != kMagic) {
        return Status::Corruption("bad magic");
    }

    // 防御性校验：避免 key_size + value_size 溢出后导致分配/读取异常。
    const uint64_t payload_size = static_cast<uint64_t>(key_size) + static_cast<uint64_t>(value_size);
    const uint64_t total_size_64 = static_cast<uint64_t>(kHeaderSize) + payload_size;
    if (payload_size > std::numeric_limits<uint32_t>::max() ||
        total_size_64 > std::numeric_limits<uint32_t>::max()) {
        return Status::Corruption("record length overflow");
    }

    uint32_t total_size = static_cast<uint32_t>(total_size_64);
    std::string buf(total_size, '\0');

    std::memcpy(buf.data(), header, kHeaderSize);

    if (key_size + value_size > 0) {
        s = ReadBytes(offset + kHeaderSize, buf.data() + kHeaderSize, key_size + value_size);
        if (!s.ok()) {
            return s;
        }
    }

    s = DecodeRecord(buf, record);
    if (!s.ok()) {
        return s;
    }

    if (record_size != nullptr) {
        *record_size = total_size;
    }

    return Status::OK();
}
/// @brief 将文件截断到指定大小，用于崩溃恢复时裁剪末尾的不完整记录。
///
/// 实现注意：直接对已打开的 fstream 调用 resize_file 在某些平台上可能失败
/// （文件锁或流缓存未 flush），因此先 flush + close fstream，调用
/// std::filesystem::resize_file 完成截断，再重新 Open 恢复可用状态。
///
/// @param size 截断后的目标文件大小（字节）。
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status DataFile::Truncate(uint64_t size) {
    std::unique_lock<std::shared_mutex> lock(file_mutex_);
    if (!file_.is_open()) {
        return Status::IOError("file not open");
    }

    // 先关闭当前流，否则某些平台下 resize_file 可能失败
    file_.flush();
    file_.close();

    std::error_code ec;
    std::filesystem::resize_file(file_path_, static_cast<std::uintmax_t>(size), ec);
    if (ec) {
        return Status::IOError("truncate failed: " + ec.message());
    }

    std::ios::openmode mode = std::ios::binary | std::ios::in;
    if (writable_) {
        mode |= std::ios::out;
    }
    file_.open(file_path_, mode);
    if (!file_.is_open()) {
        return Status::IOError("failed to reopen file after truncate");
    }

    if (writable_) {
#ifdef _WIN32
        sync_fd_ = _open(file_path_.c_str(), _O_BINARY | _O_RDWR);
#else
        sync_fd_ = ::open(file_path_.c_str(), O_RDWR);
#endif
        if (sync_fd_ < 0) {
            file_.close();
            return Status::IOError("failed to reopen sync fd after truncate");
        }
    }

    if (read_fd_ >= 0) {
#ifdef _WIN32
        _close(read_fd_);
#else
        ::close(read_fd_);
#endif
    }
#ifdef _WIN32
    read_fd_ = _open(file_path_.c_str(), _O_BINARY | _O_RDONLY);
#else
    read_fd_ = ::open(file_path_.c_str(), O_RDONLY);
#endif
    if (read_fd_ < 0) {
        return Status::IOError("failed to reopen read fd after truncate");
    }

    return Status::OK();
}
