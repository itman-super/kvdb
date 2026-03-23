#include "kv_store.h"

#include <ctime>
#include <filesystem>

KVStore::KVStore(Options options) : options_(std::move(options)) {}

KVStore::~KVStore() {
    Close();
}

Status KVStore::Open() {
    if (opened_) {
        return Status::OK();
    }

    // 确保数据库目录存在
    std::error_code ec;
    std::filesystem::create_directories(options_.db_path, ec);
    if (ec) {
        return Status::IOError("failed to create db directory: " + ec.message());
    }

    // 第一版固定只使用一个日志文件
    // 后续扩展时，这里会演进为：
    // - 扫描多个 segment
    // - 找到最新 active file
    std::string file_path = options_.db_path + "/data_1.log";
    active_file_ = std::make_unique<DataFile>(active_file_id_, file_path);

    Status s = active_file_->Open(true);
    if (!s.ok()) {
        return s;
    }

    // 启动恢复：通过扫描已有日志重建内存索引
    s = Recover();
    if (!s.ok()) {
        return s;
    }

    opened_ = true;
    return Status::OK();
}

Status KVStore::Close() {
    if (!opened_ && !active_file_) {
        return Status::OK();
    }

    if (active_file_) {
        Status s = active_file_->Close();
        if (!s.ok()) {
            return s;
        }
    }

    opened_ = false;
    return Status::OK();
}

Status KVStore::AppendRecord(const LogRecord& record, IndexEntry* entry) {
    if (!active_file_) {
        return Status::IOError("active file is null");
    }

    uint64_t offset = 0;
    uint32_t written_size = 0;

    // 所有写入都统一走 append-only 路径
    Status s = active_file_->Append(record, &offset, &written_size);
    if (!s.ok()) {
        return s;
    }

    // 如果开启了 sync_on_write，则每次写后 flush
    if (options_.sync_on_write) {
        s = active_file_->Sync();
        if (!s.ok()) {
            return s;
        }
    }

    // 回填索引项信息
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

    // 组装一条 Put 记录
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

    // 写成功后，更新内存索引到最新位置
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

    // 先查内存索引
    auto it = index_.find(key);
    if (it == index_.end()) {
        return Status::NotFound("key not found");
    }

    // 再按 offset 去磁盘读取真正的 value
    LogRecord record;
    uint32_t record_size = 0;
    (void)record_size;

    Status s = active_file_->Read(it->second.offset, &record, &record_size);
    if (!s.ok()) {
        return s;
    }

    // 理论上第一版 index_ 里不会保留 delete key
    // 这里保留这层判断，是为了接口更稳健，也便于后续多文件/恢复场景扩展
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

    // 删除不是原地删，而是写 tombstone
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

    // 内存索引中直接移除
    index_.erase(key);
    return Status::OK();
}

Status KVStore::Recover() {
    if (!active_file_) {
        return Status::IOError("active file is null");
    }

    // 恢复过程的本质：
    // 顺序扫描 append-only log，把每个 key 的“最后一个版本”恢复到 index_ 中
    uint64_t file_size = active_file_->Size();
    uint64_t offset = 0;

    while (offset < file_size) {
        LogRecord record;
        uint32_t record_size = 0;

        Status s = active_file_->Read(offset, &record, &record_size);
        if (!s.ok()) {
            // IOError：通常表示到达文件末尾，或者尾部不完整记录
            // 当前版本选择停止恢复
            if (s.code() == Status::kIOError) {
                break;
            }

            // Corruption：说明不是“正常结束”，而是记录真的坏了（例如 CRC 不匹配）
            return s;
        }

        if (record.type == RecordType::kPut) {
            // Put：覆盖 index_ 中该 key 的位置，保证总是指向最新版本
            IndexEntry entry;
            entry.file_id = active_file_id_;
            entry.offset = offset;
            entry.record_size = record_size;
            entry.value_size = static_cast<uint32_t>(record.value.size());
            entry.timestamp = record.timestamp;
            entry.tombstone = false;
            index_[record.key] = entry;
        } else if (record.type == RecordType::kDelete) {
            // Delete：从索引中移除
            index_.erase(record.key);
        } else {
            return Status::Corruption("unknown record type");
        }

        offset += record_size;
    }

    return Status::OK();
}