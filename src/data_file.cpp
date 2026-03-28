// src/data_file.cpp
#include "data_file.h"

#include <cstring>
#include <filesystem>
#include <limits>  // numeric_limits
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#else
#include <fcntl.h>
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

template <typename T>
void AppendFixed(std::string* out, const T& value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
bool ReadFixed(const std::string& buf, std::size_t* pos, T* value) {
    if (*pos + sizeof(T) > buf.size()) {
        return false;
    }
    std::memcpy(value, buf.data() + *pos, sizeof(T));
    *pos += sizeof(T);
    return true;
}

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

template <typename T>
uint32_t CRC32UpdateFixed(uint32_t crc, const T& value) {
    return CRC32Update(crc, reinterpret_cast<const char*>(&value), sizeof(T));
}

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

DataFile::DataFile(uint32_t file_id, std::string file_path)
    : file_id_(file_id), file_path_(std::move(file_path)) {}

DataFile::~DataFile() {
    Close();
}

Status DataFile::Open(bool writable) {
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

    return Status::OK();
}

Status DataFile::Close() {
#ifdef _WIN32
    if (sync_fd_ >= 0) {
        _close(sync_fd_);
        sync_fd_ = -1;
    }
#else
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

Status DataFile::Sync() {
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

uint64_t DataFile::Size() {
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

Status DataFile::ReadBytes(uint64_t offset, char* data, std::size_t len) {
    if (!file_.is_open()) {
        return Status::IOError("file not open");
    }

    // 在真正读取前做边界校验，避免 seek/read 后才发现 EOF。
    const uint64_t file_size = Size();
    if (offset > file_size) {
        return Status::OutOfRange("offset out of range: " + std::to_string(offset) +
                                  ", file_size: " + std::to_string(file_size));
    }
    if (len > file_size - offset) {
        return Status::OutOfRange("read out of range, offset: " + std::to_string(offset) +
                                  ", len: " + std::to_string(len) +
                                  ", file_size: " + std::to_string(file_size));
    }

    file_.clear();
    // seekg 失败通常意味着偏移非法或底层流状态异常。
    file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!file_) {
        return Status::IOError("seekg failed at offset: " + std::to_string(offset));
    }

    file_.read(data, static_cast<std::streamsize>(len));
    if (file_.gcount() != static_cast<std::streamsize>(len)) {
        return Status::IOError("read failed, expected: " + std::to_string(len) +
                               ", got: " + std::to_string(file_.gcount()));
    }

    return Status::OK();
}

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

Status DataFile::Append(const LogRecord& record, uint64_t* offset, uint32_t* written_size) {
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

    if (offset != nullptr) {
        *offset = static_cast<uint64_t>(pos);
    }
    if (written_size != nullptr) {
        *written_size = static_cast<uint32_t>(encoded.size());
    }

    return Status::OK();
}

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
Status DataFile::Truncate(uint64_t size) {
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

    // 重新打开，保持对象可继续使用
    Status s = Open(writable_);
    if (!s.ok()) {
        return s;
    }

    return Status::OK();
}
