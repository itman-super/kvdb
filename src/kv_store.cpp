// src/kv_store.cpp
#include "kv_store.h"

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <limits>

namespace {

// 单条记录固定头大小：magic(4)+type(1)+timestamp(8)+key_size(4)+value_size(4)+crc(4)
constexpr uint32_t kRecordHeaderSize =
    sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) +
    sizeof(uint32_t);

constexpr uint32_t kHintMagic = 0x4B564848;      // KVHH
constexpr uint32_t kSnapshotMagic = 0x4B565350;  // KVSP
constexpr uint32_t kFormatVersion = 1;

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

    Status s = file->Truncate(safe_offset);
    if (!s.ok()) {
        return s;
    }

    *action = RecoverReadAction::kStopScanCurrentFile;
    return Status::OK();
}

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

KVStore::KVStore(Options options) : options_(std::move(options)) {}

KVStore::~KVStore() {
    Close();
}

std::string KVStore::BuildDataFilePath(uint32_t file_id) const {
    return options_.db_path + "/data_" + std::to_string(file_id) + ".log";
}

std::string KVStore::BuildIndexSnapshotPath() const {
    return options_.db_path + "/index.snapshot";
}

std::string KVStore::BuildHintFilePath(uint32_t file_id) const {
    return options_.db_path + "/hint_" + std::to_string(file_id) + ".hint";
}

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

    const bool loaded_from_snapshot = TryLoadIndexSnapshot();
    Status s = loaded_from_snapshot ? Status::OK() : Recover();
    if (!s.ok()) {
        return s;
    }

    opened_ = true;
    return Status::OK();
}

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
    return SaveIndexSnapshot();
}

bool KVStore::TryLoadIndexSnapshot() {
    std::ifstream in(BuildIndexSnapshotPath(), std::ios::binary);
    if (!in.is_open()) {
        return false;
    }

    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t file_count = 0;
    uint32_t entry_count = 0;
    uint32_t snapshot_active_file_id = 0;

    in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    in.read(reinterpret_cast<char*>(&version), sizeof(version));
    in.read(reinterpret_cast<char*>(&file_count), sizeof(file_count));
    in.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));
    in.read(reinterpret_cast<char*>(&snapshot_active_file_id), sizeof(snapshot_active_file_id));
    if (!in || magic != kSnapshotMagic || version != kFormatVersion) {
        return false;
    }

    std::vector<uint32_t> snapshot_file_ids;
    snapshot_file_ids.reserve(file_count);
    for (uint32_t i = 0; i < file_count; ++i) {
        uint32_t fid = 0;
        uint64_t file_size = 0;
        int64_t file_mtime = 0;
        in.read(reinterpret_cast<char*>(&fid), sizeof(fid));
        in.read(reinterpret_cast<char*>(&file_size), sizeof(file_size));
        in.read(reinterpret_cast<char*>(&file_mtime), sizeof(file_mtime));
        if (!in) {
            return false;
        }
        snapshot_file_ids.push_back(fid);

        std::error_code ec;
        const uint64_t current_size = std::filesystem::file_size(BuildDataFilePath(fid), ec);
        if (ec || current_size != file_size) {
            return false;
        }
        const auto current_mtime = std::filesystem::last_write_time(BuildDataFilePath(fid), ec);
        if (ec) {
            return false;
        }
        const int64_t current_mtime_ticks = static_cast<int64_t>(current_mtime.time_since_epoch().count());
        if (current_mtime_ticks != file_mtime) {
            return false;
        }
    }

    std::sort(snapshot_file_ids.begin(), snapshot_file_ids.end());
    std::vector<uint32_t> current_ids = ordered_file_ids_;
    std::sort(current_ids.begin(), current_ids.end());
    if (snapshot_file_ids != current_ids || snapshot_active_file_id != active_file_id_) {
        return false;
    }

    std::unordered_map<std::string, IndexEntry> loaded_index;
    loaded_index.reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; ++i) {
        uint32_t key_size = 0;
        in.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        if (!in) {
            return false;
        }

        std::string key;
        key.resize(key_size);
        in.read(key.data(), static_cast<std::streamsize>(key_size));
        if (!in) {
            return false;
        }

        IndexEntry entry;
        in.read(reinterpret_cast<char*>(&entry.file_id), sizeof(entry.file_id));
        in.read(reinterpret_cast<char*>(&entry.offset), sizeof(entry.offset));
        in.read(reinterpret_cast<char*>(&entry.record_size), sizeof(entry.record_size));
        in.read(reinterpret_cast<char*>(&entry.value_size), sizeof(entry.value_size));
        in.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp));
        in.read(reinterpret_cast<char*>(&entry.tombstone), sizeof(entry.tombstone));
        if (!in) {
            return false;
        }

        loaded_index.emplace(std::move(key), entry);
    }

    index_ = std::move(loaded_index);
    return true;
}

Status KVStore::SaveIndexSnapshot() const {
    std::ofstream out(BuildIndexSnapshotPath(), std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        return Status::IOError("failed to open index snapshot");
    }

    const uint32_t file_count = static_cast<uint32_t>(ordered_file_ids_.size());
    const uint32_t entry_count = static_cast<uint32_t>(index_.size());

    out.write(reinterpret_cast<const char*>(&kSnapshotMagic), sizeof(kSnapshotMagic));
    out.write(reinterpret_cast<const char*>(&kFormatVersion), sizeof(kFormatVersion));
    out.write(reinterpret_cast<const char*>(&file_count), sizeof(file_count));
    out.write(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));
    out.write(reinterpret_cast<const char*>(&active_file_id_), sizeof(active_file_id_));

    for (uint32_t file_id : ordered_file_ids_) {
        std::error_code ec;
        const uint64_t file_size = std::filesystem::file_size(BuildDataFilePath(file_id), ec);
        if (ec) {
            return Status::IOError("failed to stat data file for snapshot");
        }
        const auto mtime = std::filesystem::last_write_time(BuildDataFilePath(file_id), ec);
        if (ec) {
            return Status::IOError("failed to stat data file mtime for snapshot");
        }
        const int64_t mtime_ticks = static_cast<int64_t>(mtime.time_since_epoch().count());

        out.write(reinterpret_cast<const char*>(&file_id), sizeof(file_id));
        out.write(reinterpret_cast<const char*>(&file_size), sizeof(file_size));
        out.write(reinterpret_cast<const char*>(&mtime_ticks), sizeof(mtime_ticks));
    }

    for (const auto& [key, entry] : index_) {
        const uint32_t key_size = static_cast<uint32_t>(key.size());
        out.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        out.write(key.data(), static_cast<std::streamsize>(key.size()));
        out.write(reinterpret_cast<const char*>(&entry.file_id), sizeof(entry.file_id));
        out.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
        out.write(reinterpret_cast<const char*>(&entry.record_size), sizeof(entry.record_size));
        out.write(reinterpret_cast<const char*>(&entry.value_size), sizeof(entry.value_size));
        out.write(reinterpret_cast<const char*>(&entry.timestamp), sizeof(entry.timestamp));
        out.write(reinterpret_cast<const char*>(&entry.tombstone), sizeof(entry.tombstone));
    }

    if (!out) {
        return Status::IOError("failed to write index snapshot");
    }
    return Status::OK();
}

Status KVStore::RecoverFromHintFile(uint32_t file_id) {
    std::ifstream in(BuildHintFilePath(file_id), std::ios::binary);
    if (!in.is_open()) {
        return Status::NotFound("hint file not found");
    }

    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t hint_file_id = 0;
    uint32_t entry_count = 0;
    in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    in.read(reinterpret_cast<char*>(&version), sizeof(version));
    in.read(reinterpret_cast<char*>(&hint_file_id), sizeof(hint_file_id));
    in.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));
    if (!in || magic != kHintMagic || version != kFormatVersion || hint_file_id != file_id) {
        return Status::Corruption("invalid hint file");
    }

    for (uint32_t i = 0; i < entry_count; ++i) {
        uint32_t key_size = 0;
        in.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        if (!in) {
            return Status::Corruption("invalid hint key size");
        }

        std::string key;
        key.resize(key_size);
        in.read(key.data(), static_cast<std::streamsize>(key_size));
        if (!in) {
            return Status::Corruption("invalid hint key bytes");
        }

        IndexEntry entry;
        in.read(reinterpret_cast<char*>(&entry.offset), sizeof(entry.offset));
        in.read(reinterpret_cast<char*>(&entry.record_size), sizeof(entry.record_size));
        in.read(reinterpret_cast<char*>(&entry.value_size), sizeof(entry.value_size));
        in.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp));
        if (!in) {
            return Status::Corruption("invalid hint entry");
        }

        entry.file_id = file_id;
        entry.tombstone = false;
        index_[std::move(key)] = entry;
    }

    return Status::OK();
}

uint32_t KVStore::EstimateRecordSize(const LogRecord& record) const {
    return static_cast<uint32_t>(kRecordHeaderSize + record.key.size() + record.value.size());
}

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

    const uint32_t merged_file_id = active_file_id_ + 1;
    auto merged_file = std::make_unique<DataFile>(merged_file_id, BuildDataFilePath(merged_file_id));
    Status s = merged_file->Open(true);
    if (!s.ok()) {
        return s;
    }

    std::unordered_map<std::string, IndexEntry> compacted_index;
    compacted_index.reserve(index_.size());
    struct HintRecord {
        std::string key;
        IndexEntry entry;
    };
    std::vector<HintRecord> hint_records;
    hint_records.reserve(index_.size());

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
        hint_records.push_back(HintRecord{key, new_entry});
    }

    s = merged_file->Sync();
    if (!s.ok()) {
        return s;
    }

    s = merged_file->Close();
    if (!s.ok()) {
        return s;
    }

    {
        std::ofstream hint_out(BuildHintFilePath(merged_file_id), std::ios::binary | std::ios::trunc);
        if (!hint_out.is_open()) {
            return Status::IOError("failed to create hint file");
        }

        const uint32_t entry_count = static_cast<uint32_t>(hint_records.size());
        hint_out.write(reinterpret_cast<const char*>(&kHintMagic), sizeof(kHintMagic));
        hint_out.write(reinterpret_cast<const char*>(&kFormatVersion), sizeof(kFormatVersion));
        hint_out.write(reinterpret_cast<const char*>(&merged_file_id), sizeof(merged_file_id));
        hint_out.write(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));

        for (const auto& hint : hint_records) {
            const uint32_t key_size = static_cast<uint32_t>(hint.key.size());
            hint_out.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
            hint_out.write(hint.key.data(), static_cast<std::streamsize>(hint.key.size()));
            hint_out.write(reinterpret_cast<const char*>(&hint.entry.offset), sizeof(hint.entry.offset));
            hint_out.write(reinterpret_cast<const char*>(&hint.entry.record_size), sizeof(hint.entry.record_size));
            hint_out.write(reinterpret_cast<const char*>(&hint.entry.value_size), sizeof(hint.entry.value_size));
            hint_out.write(reinterpret_cast<const char*>(&hint.entry.timestamp), sizeof(hint.entry.timestamp));
        }

        if (!hint_out) {
            return Status::IOError("failed to write hint file");
        }
    }

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
        std::filesystem::remove(BuildHintFilePath(file_id), ec);
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

    return SaveIndexSnapshot();
}

Status KVStore::Recover() {
    for (uint32_t file_id : ordered_file_ids_) {
        if (file_id != active_file_id_) {
            Status hint_status = RecoverFromHintFile(file_id);
            if (hint_status.ok()) {
                continue;
            }
            if (hint_status.code() != Status::kNotFound) {
                return hint_status;
            }
        }

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
