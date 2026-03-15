#include "data_file.h"

#include <cstring>
#include <filesystem>
#include <vector>

namespace {

// 文件记录的 magic number，用于快速识别记录是否合法
constexpr uint32_t kMagic = 0x4B564442;  // "KVDB"

// 第一版记录头布局：
// magic(4) + type(1) + timestamp(8) + key_size(4) + value_size(4)
constexpr std::size_t kHeaderSize =
    sizeof(uint32_t) +   // magic
    sizeof(uint8_t) +    // type
    sizeof(uint64_t) +   // timestamp
    sizeof(uint32_t) +   // key_size
    sizeof(uint32_t);    // value_size

// 将一个定长类型追加到字符串末尾
// 这里直接按内存字节布局写入，第一版默认本机读写，不处理跨平台字节序问题
template <typename T>
void AppendFixed(std::string* out, const T& value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(T));
}

// 从 buf 的当前位置读取一个定长类型，并推进 pos
template <typename T>
bool ReadFixed(const std::string& buf, std::size_t* pos, T* value) {
    if (*pos + sizeof(T) > buf.size()) {
        return false;
    }
    std::memcpy(value, buf.data() + *pos, sizeof(T));
    *pos += sizeof(T);
    return true;
}

}  // namespace

DataFile::DataFile(uint32_t file_id, std::string file_path)
    : file_id_(file_id), file_path_(std::move(file_path)) {}

DataFile::~DataFile() {
    Close();
}

Status DataFile::Open(bool writable) {
    writable_ = writable;

    // 避免重复打开
    if (file_.is_open()) {
        return Status::OK();
    }

    // 第一版以二进制方式打开
    // 读取必须支持；如果 writable=true，则同时支持写
    std::ios::openmode mode = std::ios::binary | std::ios::in;
    if (writable_) {
        mode |= std::ios::out;
    }

    file_.open(file_path_, mode);

    // 如果文件不存在且可写，则先创建后再打开
    if (!file_.is_open() && writable_) {
        std::ofstream create(file_path_, std::ios::binary | std::ios::out);
        create.close();
        file_.open(file_path_, mode);
    }

    if (!file_.is_open()) {
        return Status::IOError("failed to open file: " + file_path_);
    }

    return Status::OK();
}

Status DataFile::Close() {
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

    // 第一版先用 flush
    // 若后续要增强 crash consistency，需要补真正的 fsync/fdatasync
    file_.flush();
    if (!file_) {
        return Status::IOError("flush failed");
    }
    return Status::OK();
}

uint64_t DataFile::Size() {
    if (!file_.is_open()) {
        return 0;
    }

    // 清除状态位，防止上次 EOF 或 fail 状态影响 seek
    file_.clear();
    file_.seekg(0, std::ios::end);
    return static_cast<uint64_t>(file_.tellg());
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

    // 先 clear，再 seekg，是 fstream 读写混用时很关键的习惯
    file_.clear();
    file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!file_) {
        return Status::IOError("seekg failed");
    }

    file_.read(data, static_cast<std::streamsize>(len));
    if (file_.gcount() != static_cast<std::streamsize>(len)) {
        return Status::IOError("read failed or reached EOF");
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

    // 固定头
    AppendFixed(&out, magic);
    AppendFixed(&out, type);
    AppendFixed(&out, timestamp);
    AppendFixed(&out, key_size);
    AppendFixed(&out, value_size);

    // 变长 body
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

    // 依次读取头部字段
    if (!ReadFixed(buf, &pos, &magic) ||
        !ReadFixed(buf, &pos, &type) ||
        !ReadFixed(buf, &pos, &timestamp) ||
        !ReadFixed(buf, &pos, &key_size) ||
        !ReadFixed(buf, &pos, &value_size)) {
        return Status::Corruption("bad record header");
    }

    if (magic != kMagic) {
        return Status::Corruption("bad magic");
    }

    // 校验总长度是否匹配
    if (buf.size() != kHeaderSize + key_size + value_size) {
        return Status::Corruption("record size mismatch");
    }

    record->type = static_cast<RecordType>(type);
    record->timestamp = timestamp;
    record->key.assign(buf.data() + pos, key_size);
    pos += key_size;
    record->value.assign(buf.data() + pos, value_size);

    return Status::OK();
}

Status DataFile::Append(const LogRecord& record, uint64_t* offset, uint32_t* written_size) {
    if (!file_.is_open()) {
        return Status::IOError("file not open");
    }

    std::string encoded = EncodeRecord(record);

    // 追加写：始终 seek 到文件末尾
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
    // 先读固定头
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

    if (!ReadFixed(header_buf, &pos, &magic) ||
        !ReadFixed(header_buf, &pos, &type) ||
        !ReadFixed(header_buf, &pos, &timestamp) ||
        !ReadFixed(header_buf, &pos, &key_size) ||
        !ReadFixed(header_buf, &pos, &value_size)) {
        return Status::Corruption("bad header");
    }

    if (magic != kMagic) {
        return Status::Corruption("bad magic");
    }

    // 根据头部推算完整记录长度
    uint32_t total_size = static_cast<uint32_t>(kHeaderSize + key_size + value_size);
    std::string buf(total_size, '\0');

    // 拷贝头部
    std::memcpy(buf.data(), header, kHeaderSize);

    // 再读 key/value body
    if (key_size + value_size > 0) {
        s = ReadBytes(offset + kHeaderSize, buf.data() + kHeaderSize, key_size + value_size);
        if (!s.ok()) {
            return s;
        }
    }

    // 最终统一走 DecodeRecord，保证解析逻辑一致
    s = DecodeRecord(buf, record);
    if (!s.ok()) {
        return s;
    }

    if (record_size != nullptr) {
        *record_size = total_size;
    }

    return Status::OK();
}