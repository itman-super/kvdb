// src/kv_store.cpp
#include "kv_store.h"

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <limits>

namespace {

// 单条记录固定头大小：magic(4)+type(1)+timestamp(8)+key_size(4)+value_size(4)+crc(4)
constexpr uint32_t kRecordHeaderSize =
    sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) +
    sizeof(uint32_t);

// 判断文件名是否匹配 data_<id>.log，并解析出 file_id。
bool ParseDataFileId(const std::string& file_name, uint32_t* file_id) {
    constexpr const char* kPrefix = "data_";
    constexpr const char* kSuffix = ".log";

    if (file_name.size() <= 9) {
        return false;
    }
    if (file_name.rfind(kPrefix, 0) != 0) {
        return false;
    }
    if (file_name.substr(file_name.size() - 4) != kSuffix) {
        return false;
    }

    const std::string id_part = file_name.substr(5, file_name.size() - 9);
    if (id_part.empty()) {
        return false;
    }

    for (char c : id_part) {
        if (c < '0' || c > '9') {
            return false;
        }
    }

    try {
        const unsigned long parsed = std::stoul(id_part);
        if (parsed == 0 || parsed > std::numeric_limits<uint32_t>::max()) {
            return false;
        }
        *file_id = static_cast<uint32_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

// 构造：保存配置。
KVStore::KVStore(Options options) : options_(std::move(options)) {}

// 析构：确保资源释放。
KVStore::~KVStore() {
    Close();
}

// 生成 data_<id>.log 的完整路径。
std::string KVStore::BuildDataFilePath(uint32_t file_id) const {
    return options_.db_path + "/data_" + std::to_string(file_id) + ".log";
}

// 打开数据库并加载全部 segment。
Status KVStore::Open() {
    if (opened_) {
        return Status::OK();
    }

    std::error_code ec;
    std::filesystem::create_directories(options_.db_path, ec);
    if (ec) {
        return Status::IOError("failed to create db directory: " + ec.message());
    }

    data_files_.clear();
    ordered_file_ids_.clear();
    index_.clear();
    active_file_ = nullptr;

    for (const auto& entry : std::filesystem::directory_iterator(options_.db_path, ec)) {
        if (ec) {
            return Status::IOError("failed to iterate db directory: " + ec.message());
        }
        if (!entry.is_regular_file()) {
            continue;
        }

        uint32_t file_id = 0;
        if (!ParseDataFileId(entry.path().filename().string(), &file_id)) {
            continue;
        }
        ordered_file_ids_.push_back(file_id);
    }

    if (ordered_file_ids_.empty()) {
        ordered_file_ids_.push_back(1);
    }

    std::sort(ordered_file_ids_.begin(), ordered_file_ids_.end());
    ordered_file_ids_.erase(std::unique(ordered_file_ids_.begin(), ordered_file_ids_.end()),
                            ordered_file_ids_.end());

    active_file_id_ = ordered_file_ids_.back();

    for (uint32_t file_id : ordered_file_ids_) {
        const bool writable = (file_id == active_file_id_);
        auto data_file = std::make_unique<DataFile>(file_id, BuildDataFilePath(file_id));
        Status s = data_file->Open(writable);
        if (!s.ok()) {
            return s;
        }

        if (writable) {
            active_file_ = data_file.get();
        }

        data_files_[file_id] = std::move(data_file);
    }

    if (active_file_ == nullptr) {
        return Status::IOError("active file is null after open");
    }

    Status s = Recover();
    if (!s.ok()) {
        return s;
    }

    opened_ = true;
    return Status::OK();
}

// 关闭数据库。
Status KVStore::Close() {
    if (!opened_ && data_files_.empty()) {
        return Status::OK();
    }

    for (auto& [file_id, file] : data_files_) {
        (void)file_id;
        Status s = file->Close();
        if (!s.ok()) {
            return s;
        }
    }

    data_files_.clear();
    ordered_file_ids_.clear();
    active_file_ = nullptr;
    opened_ = false;
    return Status::OK();
}

// 估算记录编码大小，供 rotate 判定使用。
uint32_t KVStore::EstimateRecordSize(const LogRecord& record) const {
    return static_cast<uint32_t>(kRecordHeaderSize + record.key.size() + record.value.size());
}

// 检查是否需要切换新 segment。
Status KVStore::RotateIfNeeded(uint32_t incoming_record_size) {
    if (active_file_ == nullptr) {
        return Status::IOError("active file is null");
    }

    const std::size_t max_size = options_.max_data_file_size;
    if (max_size == 0) {
        return Status::OK();
    }

    const uint64_t current_size = active_file_->Size();
    if (current_size == 0) {
        return Status::OK();
    }

    if (current_size + incoming_record_size <= max_size) {
        return Status::OK();
    }

    return RotateActiveFile();
}

// 创建新的 active segment。
Status KVStore::RotateActiveFile() {
    if (active_file_ == nullptr) {
        return Status::IOError("active file is null");
    }

    Status s = active_file_->Sync();
    if (!s.ok()) {
        return s;
    }

    ++active_file_id_;
    auto next = std::make_unique<DataFile>(active_file_id_, BuildDataFilePath(active_file_id_));
    s = next->Open(true);
    if (!s.ok()) {
        return s;
    }

    active_file_ = next.get();
    data_files_[active_file_id_] = std::move(next);
    ordered_file_ids_.push_back(active_file_id_);
    return Status::OK();
}

// 追加记录并更新索引信息。
Status KVStore::AppendRecord(const LogRecord& record, IndexEntry* entry) {
    if (!active_file_) {
        return Status::IOError("active file is null");
    }

    Status s = RotateIfNeeded(EstimateRecordSize(record));
    if (!s.ok()) {
        return s;
    }

    uint64_t offset = 0;
    uint32_t written_size = 0;

    s = active_file_->Append(record, &offset, &written_size);
    if (!s.ok()) {
        return s;
    }

    if (options_.sync_on_write) {
        s = active_file_->Sync();
        if (!s.ok()) {
            return s;
        }
    }

    if (entry != nullptr) {
        entry->file_id = active_file_id_;
        entry->offset = offset;
        entry->record_size = written_size;
        entry->value_size = static_cast<uint32_t>(record.value.size());
        entry->timestamp = record.timestamp;
        entry->tombstone = (record.type == RecordType::kDelete);
    }

    return Status::OK();
}

// 写入 key/value。
Status KVStore::Put(const std::string& key, const std::string& value) {
    if (!opened_) {
        return Status::IOError("db not open");
    }
    if (key.empty()) {
        return Status::InvalidArgument("key is empty");
    }

    LogRecord record;
    record.type = RecordType::kPut;
    record.timestamp = static_cast<uint64_t>(std::time(nullptr));
    record.key = key;
    record.value = value;

    IndexEntry entry;
    Status s = AppendRecord(record, &entry);
    if (!s.ok()) {
        return s;
    }

    index_[key] = entry;
    return Status::OK();
}

// 读取 key。
Status KVStore::Get(const std::string& key, std::string* value) {
    if (!opened_) {
        return Status::IOError("db not open");
    }
    if (value == nullptr) {
        return Status::InvalidArgument("value output is null");
    }

    auto it = index_.find(key);
    if (it == index_.end()) {
        return Status::NotFound("key not found");
    }

    auto file_it = data_files_.find(it->second.file_id);
    if (file_it == data_files_.end()) {
        return Status::Corruption("index points to missing data file");
    }

    LogRecord record;
    uint32_t record_size = 0;
    Status s = file_it->second->Read(it->second.offset, &record, &record_size);
    if (!s.ok()) {
        return s;
    }

    if (record.type == RecordType::kDelete) {
        return Status::NotFound("key deleted");
    }

    *value = record.value;
    return Status::OK();
}

// 删除 key。
Status KVStore::Delete(const std::string& key) {
    if (!opened_) {
        return Status::IOError("db not open");
    }

    auto it = index_.find(key);
    if (it == index_.end()) {
        return Status::NotFound("key not found");
    }

    LogRecord record;
    record.type = RecordType::kDelete;
    record.timestamp = static_cast<uint64_t>(std::time(nullptr));
    record.key = key;
    record.value.clear();

    IndexEntry entry;
    Status s = AppendRecord(record, &entry);
    if (!s.ok()) {
        return s;
    }

    index_.erase(key);
    return Status::OK();
}

// 扫描所有 segment 做恢复。
Status KVStore::Recover() {
    for (uint32_t file_id : ordered_file_ids_) {
        auto it = data_files_.find(file_id);
        if (it == data_files_.end()) {
            return Status::Corruption("missing file while recover");
        }

        DataFile* file = it->second.get();
        const uint64_t file_size = file->Size();
        uint64_t offset = 0;

        while (offset < file_size) {
            LogRecord record;
            uint32_t record_size = 0;

            Status s = file->Read(offset, &record, &record_size);
            if (!s.ok()) {
                if ((s.code() == Status::kIOError || s.code() == Status::kOutOfRange) &&
                    file_id == active_file_id_) {
                    Status ts = file->Truncate(offset);
                    if (!ts.ok()) {
                        return ts;
                    }
                    break;
                }
                return s;
            }

            if (record.type == RecordType::kPut) {
                IndexEntry entry;
                entry.file_id = file_id;
                entry.offset = offset;
                entry.record_size = record_size;
                entry.value_size = static_cast<uint32_t>(record.value.size());
                entry.timestamp = record.timestamp;
                entry.tombstone = false;
                index_[record.key] = entry;
            } else if (record.type == RecordType::kDelete) {
                index_.erase(record.key);
            } else {
                return Status::Corruption("unknown record type");
            }

            offset += record_size;
        }
    }

    return Status::OK();
}
