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

bool IsRecoverableTailError(const Status& status) {
    return status.code() == Status::kIOError || status.code() == Status::kOutOfRange;
}

enum class RecoverReadAction {
    kContinueScanCurrentFile,
    kStopScanCurrentFile,
};

// 处理恢复阶段的读失败：
// - 若是可恢复的“尾部半条记录”错误，则截断到最后已知有效偏移并通知外层结束当前文件扫描；
// - 若不是可恢复错误，直接把原错误返回给调用方。
Status HandleRecoverReadFailure(DataFile* file,
                                uint64_t safe_offset,
                                const Status& read_status,
                                RecoverReadAction* action) {
    if (action == nullptr) {
        return Status::InvalidArgument("action is null");
    }

    *action = RecoverReadAction::kContinueScanCurrentFile;
    if (!IsRecoverableTailError(read_status)) {
        return read_status;
    }

    // 尾部损坏场景：截断到最后完整记录的位置，避免后续再次读取到坏尾。
    Status s = file->Truncate(safe_offset);
    if (!s.ok()) {
        return s;
    }

    *action = RecoverReadAction::kStopScanCurrentFile;
    return Status::OK();
}

// 将一条已解码记录应用到内存索引：
// - kDelete: 删除 key；
// - kPut: 更新 key -> 最新位置；
// - 其他类型：视为数据损坏。
Status ApplyRecoveredRecord(uint32_t file_id,
                            uint64_t offset,
                            uint32_t record_size,
                            const LogRecord& record,
                            std::unordered_map<std::string, IndexEntry>* index) {
    if (index == nullptr) {
        return Status::InvalidArgument("index is null");
    }

    if (record.type == RecordType::kDelete) {
        index->erase(record.key);
        return Status::OK();
    }

    if (record.type != RecordType::kPut) {
        return Status::Corruption("unknown record type");
    }

    IndexEntry entry;
    entry.file_id = file_id;
    entry.offset = offset;
    entry.record_size = record_size;
    entry.value_size = static_cast<uint32_t>(record.value.size());
    entry.timestamp = record.timestamp;
    entry.tombstone = false;
    (*index)[record.key] = entry;
    return Status::OK();
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

Status KVStore::Merge() {
    if (!opened_) {
        return Status::IOError("db not open");
    }
    if (active_file_ == nullptr) {
        return Status::IOError("active file is null");
    }

    // 先构造 merged 文件及新索引；成功后再替换现有状态。
    const uint32_t merged_file_id = active_file_id_ + 1;
    auto merged_file = std::make_unique<DataFile>(merged_file_id, BuildDataFilePath(merged_file_id));
    Status s = merged_file->Open(true);
    if (!s.ok()) {
        return s;
    }

    std::unordered_map<std::string, IndexEntry> compacted_index;
    compacted_index.reserve(index_.size());

    for (const auto& [key, entry] : index_) {
        auto file_it = data_files_.find(entry.file_id);
        if (file_it == data_files_.end()) {
            return Status::Corruption("index points to missing data file during merge");
        }

        LogRecord record;
        uint32_t record_size = 0;
        s = file_it->second->Read(entry.offset, &record, &record_size);
        if (!s.ok()) {
            return s;
        }

        if (record.type != RecordType::kPut) {
            return Status::Corruption("non-put record found in live index during merge");
        }
        if (record.key != key) {
            return Status::Corruption("key mismatch between index and record during merge");
        }

        uint64_t new_offset = 0;
        uint32_t new_size = 0;
        s = merged_file->Append(record, &new_offset, &new_size);
        if (!s.ok()) {
            return s;
        }

        IndexEntry new_entry;
        new_entry.file_id = merged_file_id;
        new_entry.offset = new_offset;
        new_entry.record_size = new_size;
        new_entry.value_size = static_cast<uint32_t>(record.value.size());
        new_entry.timestamp = record.timestamp;
        new_entry.tombstone = false;
        compacted_index[key] = new_entry;
    }

    s = merged_file->Sync();
    if (!s.ok()) {
        return s;
    }

    s = merged_file->Close();
    if (!s.ok()) {
        return s;
    }

    // 关闭并清理旧 segment 文件。
    for (auto& [file_id, file] : data_files_) {
        (void)file_id;
        s = file->Close();
        if (!s.ok()) {
            return s;
        }
    }

    for (uint32_t file_id : ordered_file_ids_) {
        std::error_code ec;
        std::filesystem::remove(BuildDataFilePath(file_id), ec);
        if (ec) {
            return Status::IOError("failed to remove old data file: " + ec.message());
        }
    }

    data_files_.clear();
    ordered_file_ids_.clear();
    active_file_ = nullptr;

    auto reopened_merged = std::make_unique<DataFile>(merged_file_id, BuildDataFilePath(merged_file_id));
    s = reopened_merged->Open(true);
    if (!s.ok()) {
        return s;
    }

    active_file_ = reopened_merged.get();
    data_files_[merged_file_id] = std::move(reopened_merged);
    ordered_file_ids_.push_back(merged_file_id);
    active_file_id_ = merged_file_id;
    index_ = std::move(compacted_index);
    return Status::OK();
}

// 扫描所有 segment 做恢复。
Status KVStore::Recover() {
    // 恢复策略（分层处理）：
    // 1) 按 file_id 升序扫描，保证“后写入覆盖先写入”的时序正确；
    // 2) 每个文件从 offset=0 顺序读取记录；
    // 3) 读失败时统一交给 HandleRecoverReadFailure：
    //    - 可恢复尾部错误：截断后结束当前文件扫描，继续后续文件；
    //    - 其他错误：立即失败并返回；
    // 4) 读成功后统一交给 ApplyRecoveredRecord 更新内存索引。
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
                RecoverReadAction action = RecoverReadAction::kContinueScanCurrentFile;
                s = HandleRecoverReadFailure(file, offset, s, &action);
                if (!s.ok()) {
                    return s;
                }

                switch (action) {
                    case RecoverReadAction::kStopScanCurrentFile:
                        break;
                    case RecoverReadAction::kContinueScanCurrentFile:
                        continue;
                }
                break;
            }

            s = ApplyRecoveredRecord(file_id, offset, record_size, record, &index_);
            if (!s.ok()) {
                return s;
            }
            offset += record_size;
        }
    }

    return Status::OK();
}
