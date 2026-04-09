// src/kv_store.cpp
//
// KVStore 核心实现文件。
// 实现了基于 Bitcask 模型的 key-value 存储引擎，主要特性：
//  - append-only 写入：所有写操作均追加到当前活跃 segment。
//  - 内存索引：key -> (file_id, offset, record_size) 映射，全量保存在内存中。
//  - 多 segment：活跃文件达到阈值后自动 rotate 到新 segment。
//  - 崩溃恢复：重启时扫描 segment 或加载 hint/snapshot 重建索引。
//  - Merge/Compaction：将多个 segment 合并为单一 segment，清理旧版本和墓碑记录。

#include "kv_store.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>

namespace {

// 单条记录固定头大小：magic(4)+type(1)+timestamp(8)+key_size(4)+value_size(4)+crc(4)
constexpr uint32_t kRecordHeaderSize =
    sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) +
    sizeof(uint32_t);

// hint 文件魔数，用于区分 hint 文件和普通数据文件。
constexpr uint32_t kHintMagic = 0x4B564848;      // KVHH
// index snapshot 文件魔数。
constexpr uint32_t kSnapshotMagic = 0x4B565350;  // KVSP
// 当前文件格式版本，加载时做兼容性检查。
constexpr uint32_t kFormatVersion = 2;

/// @brief 判断文件名是否匹配 data_<id>.log 格式，并解析出 file_id。
///
/// 规则：
///  - 必须以 "data_" 开头，以 ".log" 结尾。
///  - 中间部分必须全为数字，且解析后的值在 [1, UINT32_MAX] 范围内（0 非法）。
///
/// @param file_name 不含路径的文件名（如 "data_3.log"）。
/// @param file_id   [out] 解析出的文件 ID。
/// @return 匹配成功且解析合法时返回 true，否则返回 false。
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

/// @brief 判断恢复阶段读取失败的错误是否属于"可恢复的尾部截断"类型。
///
/// IOError 和 OutOfRange 通常由以下情况引起：
///  - 进程崩溃时最后一条记录只写了部分字节（尾部半条记录）。
///  - 磁盘空间耗尽导致写入中断。
/// 这类错误可以通过截断文件来修复，继续恢复后续 segment。
/// 而 Corruption（bad magic/type/CRC）则表示数据已损坏，应作为致命错误上报。
///
/// @param status 读取操作返回的 Status。
/// @return 若为 IOError 或 OutOfRange 则返回 true，否则返回 false。
bool IsRecoverableTailError(const Status& status) {
    return status.code() == Status::kIOError || status.code() == Status::kOutOfRange;
}

/// @brief 恢复阶段读取操作后的后续动作枚举。
enum class RecoverReadAction {
    kContinueScanCurrentFile,  ///< 继续扫描当前 segment 的后续记录。
    kStopScanCurrentFile,      ///< 停止扫描当前 segment（已截断尾部）。
};

/// @brief 处理恢复阶段读取记录失败的情况，决定是截断文件还是返回致命错误。
///
/// 若错误属于可恢复的尾部截断类型（IOError/OutOfRange）：
///  1. 将文件截断到 safe_offset（最近一条完整记录的结束位置）。
///  2. 将 action 设置为 kStopScanCurrentFile，通知调用方停止扫描当前 segment。
/// 若错误为不可恢复类型（如 Corruption/ChecksumFailed）：
///  1. 直接将错误向上传播，恢复失败。
///
/// @param file         当前正在扫描的 DataFile 指针。
/// @param safe_offset  最近一条完整记录结束后的偏移量（截断目标）。
/// @param read_status  DataFile::Read 返回的失败状态。
/// @param action       [out] 调用方下一步的扫描动作。
/// @return 成功处理（截断完成）返回 Status::OK()，不可恢复错误则返回该错误。
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

/// @brief 将一条已从磁盘读取的记录应用到内存索引。
///
/// - kDelete 记录：从索引中删除该 key（erase）。
/// - kPut 记录：用新位置信息更新（或插入）索引条目。
/// 由于日志按顺序重放，后读取的记录自然覆盖前面的旧版本，
/// 无需额外的时间戳比较，最终索引反映最新写入状态。
///
/// @param file_id     记录所在的 segment 文件 ID。
/// @param offset      记录在文件中的起始偏移。
/// @param record_size 记录的总字节大小（用于填充 IndexEntry）。
/// @param record      从磁盘读取并解码后的逻辑记录。
/// @param index       [in/out] 目标内存索引，不得为 nullptr。
/// @return 成功返回 Status::OK()，遇到未知记录类型返回 Corruption。
Status ApplyRecoveredRecord(uint32_t file_id,
                            uint64_t offset,
                            uint32_t record_size,
                            LogRecord record,
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
    entry.key_size = static_cast<uint32_t>(record.key.size());
    entry.timestamp = record.timestamp;
    entry.record_type = record.type;
    entry.tombstone = false;
    index->insert_or_assign(std::move(record.key), entry);
    return Status::OK();
}

}  // namespace

KVStore::Iterator::Iterator(std::vector<std::pair<std::string, std::string>> items)
    : items_(std::move(items)) {}

bool KVStore::Iterator::Valid() const {
    return index_ < items_.size();
}

void KVStore::Iterator::Next() {
    if (Valid()) {
        ++index_;
    }
}

const std::string& KVStore::Iterator::Key() const {
    if (!Valid()) {
        throw std::out_of_range("iterator is invalid");
    }
    return items_[index_].first;
}

const std::string& KVStore::Iterator::Value() const {
    if (!Valid()) {
        throw std::out_of_range("iterator is invalid");
    }
    return items_[index_].second;
}

/// @brief 构造 KVStore，仅保存配置项，不打开任何文件。
/// 调用方必须显式调用 Open() 才能使用数据库。
KVStore::KVStore(Options options) : options_(std::move(options)) {}

/// @brief 析构时自动关闭数据库，确保文件句柄被释放、索引快照被写入。
KVStore::~KVStore() {
    StopBackgroundWorker();
    Close();
}

/// @brief 启动后台工作线程（若配置了定时 Sync 或定时 Merge）。
///
/// 若后台线程已在运行（joinable），或两项后台功能均未开启，则直接返回。
/// 线程入口为 BackgroundWorkerLoop()，在后台持续等待并定期执行 Sync/Merge。
void KVStore::StartBackgroundWorker() {
    if (background_worker_.joinable()) {
        return;
    }

    if (!options_.enable_background_sync && !options_.enable_background_merge) {
        return;
    }

    stop_background_worker_ = false;
    background_worker_ = std::thread(&KVStore::BackgroundWorkerLoop, this);
}

/// @brief 通知后台工作线程停止，并阻塞等待其退出。
///
/// 通过向 background_cv_ 发送通知、设置 stop_background_worker_ = true
/// 来唤醒并终止 BackgroundWorkerLoop()，然后 join 线程。
/// 若线程未运行（not joinable），此调用为空操作。
void KVStore::StopBackgroundWorker() {
    {
        std::lock_guard<std::mutex> lk(background_mutex_);
        stop_background_worker_ = true;
    }
    background_cv_.notify_all();
    if (background_worker_.joinable()) {
        background_worker_.join();
    }
}

Status KVStore::EnqueueWriteRequest(std::vector<WriteBatchOp> ops) {
    if (!opened_.load()) {
        return Status::IOError("db not open");
    }

    WriteRequest req;
    req.ops = std::move(ops);
    std::future<Status> done = req.done.get_future();
    req.sequence = enqueued_sequence_.fetch_add(1) + 1;
    {
        std::lock_guard<std::mutex> lk(write_queue_mutex_);
        write_queue_.push_back(std::move(req));
    }
    write_queue_cv_.notify_one();
    return done.get();
}

void KVStore::WaitForAppliedSequence(uint64_t sequence) {
    std::unique_lock<std::mutex> lk(applied_sequence_mutex_);
    applied_sequence_cv_.wait(lk, [this, sequence]() {
        return applied_sequence_.load() >= sequence || !opened_.load();
    });
}

void KVStore::StartWriteWorker() {
    if (write_worker_.joinable()) {
        return;
    }
    stop_write_worker_ = false;
    write_worker_ = std::thread(&KVStore::WriteWorkerLoop, this);
}

void KVStore::StopWriteWorker() {
    {
        std::lock_guard<std::mutex> lk(write_queue_mutex_);
        stop_write_worker_ = true;
    }
    write_queue_cv_.notify_all();
    if (write_worker_.joinable()) {
        write_worker_.join();
    }
}

Status KVStore::ProcessWriteRequest(const WriteRequest& request) {
    std::lock_guard<std::mutex> append_lock(append_mutex_);
    for (const auto& op : request.ops) {
        if (op.key.empty()) {
            return Status::InvalidArgument("batch op key is empty");
        }
        if (op.type == RecordType::kPut) {
            LogRecord record{RecordType::kPut, static_cast<uint64_t>(std::time(nullptr)), op.key, op.value};
            IndexEntry entry;
            Status s = AppendRecord(record, &entry, false);
            if (!s.ok()) {
                return s;
            }
            std::unique_lock<std::shared_mutex> index_lock(index_mutex_);
            index_.insert_or_assign(op.key, entry);
            continue;
        }
        if (op.type == RecordType::kDelete) {
            {
                std::shared_lock<std::shared_mutex> index_lock(index_mutex_);
                if (index_.find(op.key) == index_.end()) {
                    continue;
                }
            }
            LogRecord record{RecordType::kDelete, static_cast<uint64_t>(std::time(nullptr)), op.key, ""};
            IndexEntry entry;
            Status s = AppendRecord(record, &entry, false);
            if (!s.ok()) {
                return s;
            }
            std::unique_lock<std::shared_mutex> index_lock(index_mutex_);
            index_.erase(op.key);
            continue;
        }
        return Status::InvalidArgument("unknown batch op type");
    }

    if (options_.sync_on_write && active_file_ != nullptr) {
        return active_file_->Sync();
    }
    return Status::OK();
}

void KVStore::WriteWorkerLoop() {
    while (true) {
        std::vector<WriteRequest> batch;
        {
            std::unique_lock<std::mutex> lk(write_queue_mutex_);
            write_queue_cv_.wait(lk, [this]() { return stop_write_worker_ || !write_queue_.empty(); });
            if (stop_write_worker_ && write_queue_.empty()) {
                return;
            }
            while (!write_queue_.empty()) {
                batch.push_back(std::move(write_queue_.front()));
                write_queue_.pop_front();
            }
        }

        for (auto& req : batch) {
            Status s = ProcessWriteRequest(req);
            req.done.set_value(s);
            applied_sequence_.store(req.sequence);
            applied_sequence_cv_.notify_all();
        }
    }
}

/// @brief 后台工作线程主循环。
///
/// 循环逻辑：
///  1. 计算下次唤醒等待时间（取 sync_interval 和 merge_interval 的较小值）。
///  2. 通过 condition_variable::wait_for 阻塞等待，直到超时或收到停止信号。
///  3. 收到停止信号则退出循环。
///  4. 若启用了定时 Sync，且距上次 Sync 已超过 background_sync_interval_ms，
///     则对当前活跃文件执行一次 Sync。
///  5. 若启用了定时 Merge，且距上次 Merge 已超过 background_merge_interval_ms，
///     且当前 segment 数量 >= background_merge_min_segments，则执行一次 MergeUnlocked。
///
/// 执行 Sync/Merge 时持有 store_mutex_ 独占锁，确保线程安全。
void KVStore::BackgroundWorkerLoop() {
    using Clock = std::chrono::steady_clock;
    auto last_sync = Clock::now();
    auto last_merge = Clock::now();

    while (true) {
        std::size_t wait_ms = 1000;
        if (options_.enable_background_sync) {
            wait_ms = std::min(wait_ms, std::max<std::size_t>(1, options_.background_sync_interval_ms));
        }
        if (options_.enable_background_merge) {
            wait_ms = std::min(wait_ms, std::max<std::size_t>(1, options_.background_merge_interval_ms));
        }

        std::unique_lock<std::mutex> lk(background_mutex_);
        background_cv_.wait_for(lk, std::chrono::milliseconds(wait_ms), [this]() {
            return stop_background_worker_;
        });
        if (stop_background_worker_) {
            return;
        }
        lk.unlock();

        const auto now = Clock::now();

        if (options_.enable_background_sync &&
            now - last_sync >= std::chrono::milliseconds(options_.background_sync_interval_ms)) {
            std::unique_lock<std::shared_mutex> store_lock(store_mutex_);
            if (opened_.load() && active_file_ != nullptr) {
                (void)active_file_->Sync();
            }
            last_sync = now;
        }

        if (options_.enable_background_merge &&
            now - last_merge >= std::chrono::milliseconds(options_.background_merge_interval_ms)) {
            std::unique_lock<std::shared_mutex> store_lock(store_mutex_);
            if (opened_.load() && data_files_.size() >= options_.background_merge_min_segments) {
                (void)MergeUnlocked();
            }
            last_merge = now;
        }
    }
}

/// @brief 生成指定 file_id 对应的 segment 文件完整路径。
/// 格式：<db_path>/data_<file_id>.log
std::string KVStore::BuildDataFilePath(uint32_t file_id) const {
    return options_.db_path + "/data_" + std::to_string(file_id) + ".log";
}

/// @brief 生成 index snapshot 文件的完整路径。
/// 格式：<db_path>/index.snapshot
std::string KVStore::BuildIndexSnapshotPath() const {
    return options_.db_path + "/index.snapshot";
}

/// @brief 生成指定 file_id 对应的 hint 文件完整路径。
/// hint 文件是 Merge 后产出的轻量索引文件，格式：<db_path>/hint_<file_id>.hint
std::string KVStore::BuildHintFilePath(uint32_t file_id) const {
    return options_.db_path + "/hint_" + std::to_string(file_id) + ".hint";
}

/// @brief 打开数据库，完成目录创建、segment 文件发现与打开、索引恢复等全部初始化工作。
///
/// 详细步骤：
///  1. 幂等检查：若已打开则直接返回 OK。
///  2. 创建 db_path 目录（若不存在）。
///  3. 扫描目录，收集所有匹配 data_<id>.log 的文件并排序去重。
///     若目录为空则从 file_id = 1 开始（首次启动）。
///  4. 将最大 file_id 对应的文件设为活跃文件（active），其余文件以只读方式打开。
///  5. 尝试加载 index snapshot（TryLoadIndexSnapshot）：
///     - 若 snapshot 有效且文件元数据未变化，直接使用 snapshot 中的索引跳过扫描。
///     - 否则调用 Recover() 从 segment/hint 文件重建索引。
///  6. 置 opened_ = true。
///
/// @return 成功返回 Status::OK()，失败返回 IOError 或 Corruption。
Status KVStore::Open() {
    std::unique_lock<std::shared_mutex> lock(store_mutex_);
    if (opened_.load()) {
        return Status::OK();
    }

    std::error_code ec;
    std::filesystem::create_directories(options_.db_path, ec);
    if (ec) {
        return Status::IOError("failed to create db directory: " + ec.message());
    }

    std::unique_lock<std::shared_mutex> files_lock(files_mutex_);
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
        auto data_file = std::make_shared<DataFile>(file_id, BuildDataFilePath(file_id));
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
    files_lock.unlock();

    const bool loaded_from_snapshot = TryLoadIndexSnapshot();
    Status s = loaded_from_snapshot ? Status::OK() : Recover();
    if (!s.ok()) {
        return s;
    }

    opened_.store(true);
    StartWriteWorker();
    StartBackgroundWorker();
    return Status::OK();
}

/// @brief 关闭数据库，释放所有文件句柄，并将当前内存索引持久化为 snapshot。
///
/// 步骤：
///  1. 幂等检查：若未打开且无文件则直接返回 OK。
///  2. 依次关闭所有 segment（包括活跃文件）。
///  3. 清空内存数据结构（data_files_、ordered_file_ids_、active_file_）。
///  4. 置 opened_ = false 后，调用 SaveIndexSnapshot() 将索引写入快照文件，
///     加速下次启动的索引重建。
///
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status KVStore::Close() {
    StopWriteWorker();
    StopBackgroundWorker();
    std::unique_lock<std::shared_mutex> lock(store_mutex_);
    if (!opened_.load() && data_files_.empty()) {
        return Status::OK();
    }

    std::unique_lock<std::shared_mutex> files_lock(files_mutex_);
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
    opened_.store(false);
    return SaveIndexSnapshot();
}

/// @brief 尝试从 index.snapshot 文件快速恢复内存索引，避免全量扫描 segment。
///
/// snapshot 文件格式：
///   [magic: 4B][version: 4B][file_count: 4B][entry_count: 4B][active_file_id: 4B]
///   对每个 segment：[file_id: 4B][file_size: 8B][mtime_ticks: 8B]
///   对每个索引条目：[key_size: 4B][key: key_size B][IndexEntry 各字段]
///
/// 有效性验证：
///  - magic 和 version 必须匹配。
///  - snapshot 中记录的 segment 集合（file_id、file_size、mtime）必须与
///    当前磁盘上的文件完全一致，任何差异均视为快照过期（返回 false）。
///  - active_file_id 必须与当前值匹配。
/// 若任一验证失败，返回 false（不视为错误），调用方将回退到 Recover() 重建索引。
///
/// @return snapshot 有效且成功加载返回 true，否则返回 false。
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
    if (!in || magic != kSnapshotMagic || (version != 1 && version != kFormatVersion)) {
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
        if (version >= 2) {
            in.read(reinterpret_cast<char*>(&entry.key_size), sizeof(entry.key_size));
        } else {
            entry.key_size = key_size;
        }
        in.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp));
        if (version >= 2) {
            uint8_t stored_record_type = static_cast<uint8_t>(RecordType::kPut);
            in.read(reinterpret_cast<char*>(&stored_record_type), sizeof(stored_record_type));
            entry.record_type = static_cast<RecordType>(stored_record_type);
        } else {
            entry.record_type = RecordType::kPut;
        }
        in.read(reinterpret_cast<char*>(&entry.tombstone), sizeof(entry.tombstone));
        if (!in) {
            return false;
        }

        loaded_index.emplace(std::move(key), entry);
    }

    index_ = std::move(loaded_index);
    return true;
}

/// @brief 将当前内存索引持久化为 index.snapshot 文件。
///
/// snapshot 文件格式与 TryLoadIndexSnapshot 中描述的格式完全对应。
/// 每次 Close() 时自动调用，Merge() 结束时也会调用以确保快照与新 segment 同步。
/// 若写入失败，仅影响下次启动速度（需回退到全量 Recover），不影响数据正确性。
///
/// @return 成功返回 Status::OK()，失败返回 IOError。
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
        out.write(reinterpret_cast<const char*>(&entry.key_size), sizeof(entry.key_size));
        out.write(reinterpret_cast<const char*>(&entry.timestamp), sizeof(entry.timestamp));
        const uint8_t record_type = static_cast<uint8_t>(entry.record_type);
        out.write(reinterpret_cast<const char*>(&record_type), sizeof(record_type));
        out.write(reinterpret_cast<const char*>(&entry.tombstone), sizeof(entry.tombstone));
    }

    if (!out) {
        return Status::IOError("failed to write index snapshot");
    }
    return Status::OK();
}

/// @brief 从指定 file_id 的 hint 文件快速重建该 segment 的索引，
///        比直接重放 segment 的开销小得多（无需读取 value）。
///
/// hint 文件格式：
///   [magic: 4B][version: 4B][file_id: 4B][entry_count: 4B]
///   对每个条目：[key_size: 4B][key: key_size B]
///               [offset: 8B][record_size: 4B][value_size: 4B][timestamp: 8B]
///
/// hint 文件只由 Merge() 产出，且仅包含 Put 记录（已清除 tombstone 和旧版本）。
/// 若 hint 文件不存在，返回 Status::NotFound，调用方会回退到扫描原始 segment。
///
/// @param file_id 要恢复的 segment 文件 ID，同时也是 hint 文件名的一部分。
/// @return 成功返回 Status::OK()，文件不存在返回 NotFound，格式损坏返回 Corruption。
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
    if (!in || magic != kHintMagic || (version != 1 && version != kFormatVersion) ||
        hint_file_id != file_id) {
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
        if (version >= 2) {
            in.read(reinterpret_cast<char*>(&entry.key_size), sizeof(entry.key_size));
        } else {
            entry.key_size = key_size;
        }
        in.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp));
        if (version >= 2) {
            uint8_t stored_record_type = static_cast<uint8_t>(RecordType::kPut);
            in.read(reinterpret_cast<char*>(&stored_record_type), sizeof(stored_record_type));
            entry.record_type = static_cast<RecordType>(stored_record_type);
        } else {
            entry.record_type = RecordType::kPut;
        }
        if (!in) {
            return Status::Corruption("invalid hint entry");
        }

        entry.file_id = file_id;
        entry.tombstone = false;
        index_[std::move(key)] = entry;
    }

    return Status::OK();
}

/// @brief 估算一条记录序列化后占用的字节数（用于 rotate 阈值判断）。
///
/// 结果 = 固定头大小 + key.size() + value.size()。
/// 这是一个保守估计（实际可能等于此值），足以决定是否需要 rotate。
///
/// @param record 待估算的逻辑记录。
/// @return 估算的序列化字节数。
uint32_t KVStore::EstimateRecordSize(const LogRecord& record) const {
    return static_cast<uint32_t>(kRecordHeaderSize + record.key.size() + record.value.size());
}

/// @brief 检查活跃 segment 是否需要 rotate，若需要则执行。
///
/// 触发条件：
///  - max_data_file_size > 0（0 表示不限制大小）。
///  - 活跃文件当前大小 + incoming_record_size 超过 max_data_file_size。
///  - 空文件（size == 0）不触发 rotate，避免创建一个空 segment 后立刻再创建新的。
///
/// @param incoming_record_size 即将写入的记录估算大小（字节）。
/// @return 无需 rotate 或 rotate 成功返回 Status::OK()，失败返回 IOError。
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

/// @brief 将当前活跃 segment 封存，并创建新的活跃 segment。
///
/// 步骤：
///  1. Sync 当前活跃文件，确保数据持久化。
///  2. 将 active_file_id_ 自增，并创建新的 DataFile 对象。
///  3. 以可写模式打开新文件（若不存在则自动创建）。
///  4. 更新 active_file_、data_files_ 和 ordered_file_ids_。
///
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status KVStore::RotateActiveFile() {
    std::unique_lock<std::shared_mutex> files_lock(files_mutex_);
    if (active_file_ == nullptr) {
        return Status::IOError("active file is null");
    }

    Status s = active_file_->Sync();
    if (!s.ok()) {
        return s;
    }

    ++active_file_id_;
    auto next = std::make_shared<DataFile>(active_file_id_, BuildDataFilePath(active_file_id_));
    s = next->Open(true);
    if (!s.ok()) {
        return s;
    }

    active_file_ = next.get();
    data_files_[active_file_id_] = std::move(next);
    ordered_file_ids_.push_back(active_file_id_);
    return Status::OK();
}

/// @brief 统一的记录追加入口，处理 rotate 判断、实际写入和 sync_on_write 逻辑。
///
/// 所有写操作（Put/Delete）最终都通过此函数完成，流程：
///  1. 检查是否需要 rotate（RotateIfNeeded）。
///  2. 调用 active_file_->Append() 追加编码后的记录，获取写入偏移和大小。
///  3. 若 sync_on_write == true，立即对活跃文件执行 Sync。
///  4. 将写入位置信息填充到 IndexEntry（供调用方更新内存索引）。
///
/// @param record  要追加的逻辑记录（kPut 或 kDelete）。
/// @param entry   [out] 填充记录位置信息（file_id、offset 等），可为 nullptr。
/// @return 成功返回 Status::OK()，失败返回 IOError。
Status KVStore::AppendRecord(const LogRecord& record, IndexEntry* entry, bool sync_immediately) {
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

    if (sync_immediately) {
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
        entry->key_size = static_cast<uint32_t>(record.key.size());
        entry->timestamp = record.timestamp;
        entry->record_type = record.type;
        entry->tombstone = (record.type == RecordType::kDelete);
    }

    return Status::OK();
}

/// @brief 写入或覆盖一个 key-value 对。
///
/// 写入路径：
///  1. 验证数据库已打开且 key 非空。
///  2. 构造 kPut 类型的 LogRecord，timestamp 使用当前 Unix 时间。
///  3. 调用 AppendRecord() 追加到活跃 segment。
///  4. 用返回的 IndexEntry 更新（或插入）内存索引 index_[key]。
///
/// 若 key 已存在，旧版本记录仍留在磁盘，只有通过 Merge() 才会被清理。
///
/// @param key   非空的键。
/// @param value 对应的值（可为空）。
/// @return 成功返回 Status::OK()，key 为空返回 InvalidArgument，其他失败返回 IOError。
Status KVStore::Put(const std::string& key, const std::string& value) {
    if (key.empty()) {
        return Status::InvalidArgument("key is empty");
    }
    WriteBatchOp op;
    op.type = RecordType::kPut;
    op.key = key;
    op.value = value;
    return EnqueueWriteRequest({std::move(op)});
}

/// @brief 读取指定 key 的 value。
///
/// 读取路径：
///  1. 验证数据库已打开且 value 输出指针非空。
///  2. 在内存索引 index_ 中查找 key。
///  3. 根据 IndexEntry 中的 file_id 定位到对应的 DataFile。
///  4. 调用 DataFile::Read() 从磁盘读取记录并验证 CRC。
///  5. 若读到的是 kDelete 记录（理论上索引不应指向 tombstone，但作为防御），返回 NotFound。
///
/// @param key   要查询的键。
/// @param value [out] 读取到的值写入此处，不得为 nullptr。
/// @return 成功返回 Status::OK()；key 不存在返回 NotFound；索引指向缺失文件返回 Corruption。
Status KVStore::Get(const std::string& key, std::string* value) {
    if (!opened_.load()) {
        return Status::IOError("db not open");
    }
    if (value == nullptr) {
        return Status::InvalidArgument("value output is null");
    }

    IndexEntry entry;
    {
        std::shared_lock<std::shared_mutex> index_lock(index_mutex_);
        auto it = index_.find(key);
        if (it == index_.end()) {
            return Status::NotFound("key not found");
        }
        entry = it->second;
    }

    return ReadValueByEntry(entry, value);
}

Status KVStore::GetWithConsistency(const std::string& key,
                                   std::string* value,
                                   ReadConsistency consistency) {
    if (consistency == ReadConsistency::kLinearizable) {
        WaitForAppliedSequence(enqueued_sequence_.load());
    }
    return Get(key, value);
}

/// @brief 删除指定 key（通过写入 tombstone 记录实现）。
///
/// Bitcask 模型的删除是"软删除"：
///  1. 检查 key 是否在内存索引中，不存在则返回 NotFound。
///  2. 构造 kDelete 类型的 LogRecord（value 为空），追加到活跃 segment。
///  3. 从内存索引 index_ 中删除该 key 的条目。
///
/// tombstone 记录会保留在磁盘，直到 Merge() 时才会被真正清除。
///
/// @param key 要删除的键。
/// @return 成功返回 Status::OK()；key 不存在返回 NotFound；写入失败返回 IOError。
Status KVStore::Delete(const std::string& key) {
    if (key.empty()) {
        return Status::InvalidArgument("key is empty");
    }
    WriteBatchOp op;
    op.type = RecordType::kDelete;
    op.key = key;
    return EnqueueWriteRequest({std::move(op)});
}

/// @brief 以原子方式顺序执行一组 Put/Delete 混合写入操作。
///
/// 批处理流程：
///  1. 验证数据库已打开。
///  2. 遍历操作列表，对每条操作：
///     - 若 key 为空，返回 InvalidArgument。
///     - kPut 操作：追加 Put 记录并更新内存索引。
///     - kDelete 操作：若 key 存在则追加 tombstone 并从索引移除；不存在则跳过。
///     - 未知操作类型：返回 InvalidArgument。
///  3. 任一操作失败则立即返回，已执行的操作**不会**回滚（非事务型批写）。
///
/// @param ops 要执行的操作列表，每项包含 type / key / value。
/// @return 成功返回 Status::OK()；key 为空或类型非法返回 InvalidArgument；
///         I/O 失败返回 IOError。
Status KVStore::WriteBatch(const std::vector<WriteBatchOp>& ops) {
    return EnqueueWriteRequest(ops);
}


/// @brief 创建一个按 key 升序排列的快照迭代器。
///
/// 实现步骤：
///  1. 在持有读锁时，将当前内存索引中的所有条目按 key 升序排序。
///  2. 逐 key 从磁盘读取对应 value（ReadValueByEntry）。
///  3. 将所有 (key, value) 对封装为 Iterator 返回。
///
/// 注意：迭代器是基于创建时刻的索引快照，不反映创建后的写入变化。
/// 创建迭代器的开销与当前 key 数量成正比（需要全量读磁盘）。
///
/// @param iter [out] 创建的迭代器对象写入此处，不得为 nullptr。
/// @return 成功返回 Status::OK()；iter 为 nullptr 返回 InvalidArgument；读取失败返回相应错误。
Status KVStore::NewIterator(std::unique_ptr<Iterator>* iter) {
    if (!opened_.load()) {
        return Status::IOError("db not open");
    }
    if (iter == nullptr) {
        return Status::InvalidArgument("iter is null");
    }

    std::vector<std::pair<std::string, IndexEntry>> entries;
    {
        std::shared_lock<std::shared_mutex> index_lock(index_mutex_);
        entries.reserve(index_.size());
        for (const auto& [key, entry] : index_) {
            entries.push_back({key, entry});
        }
    }
    std::sort(entries.begin(),
              entries.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    std::vector<std::pair<std::string, std::string>> items;
    items.reserve(entries.size());
    for (const auto& [key, entry] : entries) {
        std::string value;
        Status s = ReadValueByEntry(entry, &value);
        if (!s.ok()) {
            return s;
        }
        items.push_back({key, std::move(value)});
    }

    *iter = std::make_unique<Iterator>(std::move(items));
    return Status::OK();
}

/// @brief 按前缀扫描 key，返回所有匹配的键值对（按 key 升序）。
///
/// 扫描流程：
///  1. 从内存索引中筛选出所有以 prefix 为前缀的 key（空前缀匹配所有 key）。
///  2. 对筛出的 key 升序排序。
///  3. 逐个从磁盘读取对应 value，追加到 result。
///  4. 若 limit > 0 且已收集 limit 条记录，提前停止。
///
/// @param prefix  key 前缀过滤字符串，空串表示返回所有 key。
/// @param limit   最多返回的条目数，0 表示不限制。
/// @param result  [out] 匹配的键值对列表，调用前无需清空（函数内部会 clear）。
///                不得为 nullptr。
/// @return 成功返回 Status::OK()；result 为 nullptr 返回 InvalidArgument；
///         读取失败返回对应错误。
Status KVStore::Scan(const std::string& prefix,
                     std::size_t limit,
                     std::vector<std::pair<std::string, std::string>>* result) {
    if (!opened_.load()) {
        return Status::IOError("db not open");
    }
    if (result == nullptr) {
        return Status::InvalidArgument("result is null");
    }

    result->clear();
    std::vector<std::string> keys;
    std::unordered_map<std::string, IndexEntry> snapshot;
    {
        std::shared_lock<std::shared_mutex> index_lock(index_mutex_);
        keys.reserve(index_.size());
        for (const auto& [key, _] : index_) {
            if (key.rfind(prefix, 0) == 0) {
                keys.push_back(key);
            }
        }
        snapshot = index_;
    }
    std::sort(keys.begin(), keys.end());

    for (const auto& key : keys) {
        auto it = snapshot.find(key);
        if (it == snapshot.end()) {
            continue;
        }
        std::string value;
        Status s = ReadValueByEntry(it->second, &value);
        if (!s.ok()) {
            return s;
        }
        result->push_back({key, std::move(value)});
        if (limit > 0 && result->size() >= limit) {
            break;
        }
    }
    return Status::OK();
}

/// @brief 按 key 升序遍历所有键值对，对每对调用用户提供的回调函数。
///
/// 遍历流程：
///  1. 将内存索引中所有 key 收集后升序排序。
///  2. 逐个读取 value，调用 fn(key, value)。
///  3. 若 fn 返回非 OK 状态，立即停止遍历并将该状态向上传递。
///
/// @param fn 每对 (key, value) 的处理回调。若返回非 OK，遍历中止。不得为空（null）。
/// @return 成功遍历全部返回 Status::OK()；fn 为空返回 InvalidArgument；
///         fn 返回非 OK 时透传该状态；读取失败返回对应 I/O 错误。
Status KVStore::Fold(const std::function<Status(const std::string&, const std::string&)>& fn) {
    if (!opened_.load()) {
        return Status::IOError("db not open");
    }
    if (!fn) {
        return Status::InvalidArgument("fn is empty");
    }

    std::vector<std::string> keys;
    std::unordered_map<std::string, IndexEntry> snapshot;
    {
        std::shared_lock<std::shared_mutex> index_lock(index_mutex_);
        keys.reserve(index_.size());
        for (const auto& [key, _] : index_) {
            keys.push_back(key);
        }
        snapshot = index_;
    }
    std::sort(keys.begin(), keys.end());

    for (const auto& key : keys) {
        auto it = snapshot.find(key);
        if (it == snapshot.end()) {
            continue;
        }
        std::string value;
        Status s = ReadValueByEntry(it->second, &value);
        if (!s.ok()) {
            return s;
        }
        s = fn(key, value);
        if (!s.ok()) {
            return s;
        }
    }
    return Status::OK();
}

/// @brief 根据内存索引项（IndexEntry）从对应 segment 文件读取 value。
///
/// 步骤：
///  1. 在 data_files_ 中按 entry.file_id 查找对应 DataFile。
///  2. 调用 DataFile::Read(entry.offset, ...) 读取完整记录并验证 CRC。
///  3. 若读取到 kDelete 记录（理论上活跃索引不应指向 tombstone），
///     作为防御返回 NotFound。
///
/// 此函数为内部辅助方法，调用方（Get/NewIterator/Scan/Fold）负责持有适当的锁。
///
/// @param entry  内存索引条目，包含 file_id、offset 等位置信息。
/// @param value  [out] 读取到的 value，不得为 nullptr。
/// @return 成功返回 Status::OK()；文件缺失返回 Corruption；
///         记录被标记为删除返回 NotFound；读取失败返回对应错误。
Status KVStore::ReadValueByEntry(const IndexEntry& entry, std::string* value) {
    if (value == nullptr) {
        return Status::InvalidArgument("value output is null");
    }

    std::shared_ptr<DataFile> file;
    {
        std::shared_lock<std::shared_mutex> files_lock(files_mutex_);
        auto file_it = data_files_.find(entry.file_id);
        if (file_it == data_files_.end()) {
            return Status::Corruption("index points to missing data file");
        }
        file = file_it->second;
    }

    LogRecord record;
    uint32_t record_size = 0;
    Status s = file->Read(entry.offset, &record, &record_size);
    if (!s.ok()) {
        return s;
    }
    if (record.type == RecordType::kDelete) {
        return Status::NotFound("key deleted");
    }
    *value = std::move(record.value);
    return Status::OK();
}

/// @brief 执行 Merge/Compaction，将全部"存活" key 合并到单一新 segment。
///
/// Merge 步骤：
///  1. 创建 merged_file_id = active_file_id_ + 1 的新 segment 文件。
///  2. 遍历当前内存索引（index_），对每个 key 从原 segment 读取最新 Put 记录。
///  3. 将所有存活记录写入新 segment，更新 compacted_index。
///  4. 同步并关闭新 segment，写出对应的 hint 文件（加速后续重启恢复）。
///  5. 关闭所有旧 segment，删除旧的 data 文件和 hint 文件。
///  6. 重新打开新 segment 作为活跃文件，替换内存索引和数据结构。
///  7. 调用 SaveIndexSnapshot() 持久化新的索引快照。
///
/// 执行 Merge 后：磁盘上只剩一个 segment 文件（merged_file），
/// 所有旧版本记录和 tombstone 均已被清除。
///
/// @return 成功返回 Status::OK()，失败返回 IOError 或 Corruption。
Status KVStore::Merge() {
    std::unique_lock<std::shared_mutex> lock(store_mutex_);
    return MergeUnlocked();
}

/// @brief 在已持有 store_mutex_ 独占锁的情况下执行 Merge/Compaction。
///
/// 该方法是 Merge() 的实际实现，提取为独立函数以便后台任务线程复用（无需二次加锁）。
/// 详细步骤与 Merge() 的文档相同，参见 KVStore::Merge 的注释。
///
/// @return 成功返回 Status::OK()，失败返回 IOError 或 Corruption。
Status KVStore::MergeUnlocked() {
    std::unique_lock<std::shared_mutex> files_lock(files_mutex_);
    std::unique_lock<std::shared_mutex> index_lock(index_mutex_);
    if (!opened_.load()) {
        return Status::IOError("db not open");
    }
    if (active_file_ == nullptr) {
        return Status::IOError("active file is null");
    }

    const uint32_t merged_file_id = active_file_id_ + 1;
    auto merged_file = std::make_shared<DataFile>(merged_file_id, BuildDataFilePath(merged_file_id));
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
        new_entry.key_size = static_cast<uint32_t>(record.key.size());
        new_entry.timestamp = record.timestamp;
        new_entry.record_type = record.type;
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
            hint_out.write(reinterpret_cast<const char*>(&hint.entry.key_size), sizeof(hint.entry.key_size));
            hint_out.write(reinterpret_cast<const char*>(&hint.entry.timestamp), sizeof(hint.entry.timestamp));
            const uint8_t record_type = static_cast<uint8_t>(hint.entry.record_type);
            hint_out.write(reinterpret_cast<const char*>(&record_type), sizeof(record_type));
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

    auto reopened_merged = std::make_shared<DataFile>(merged_file_id, BuildDataFilePath(merged_file_id));
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

/// @brief 从所有 segment 文件扫描重建内存索引（全量恢复路径）。
///
/// 算法：
///  1. 按 file_id 升序遍历所有 segment（ordered_file_ids_）。
///  2. 对于非活跃 segment：优先尝试读取 hint 文件（RecoverFromHintFile）加速恢复；
///     若 hint 不存在（NotFound），则回退到扫描原始 segment；
///     若 hint 文件损坏（非 NotFound 错误），直接返回失败。
///  3. 活跃 segment 不使用 hint（hint 只在 Merge 时生成），始终全量扫描。
///  4. 逐条读取记录（DataFile::Read），通过 ApplyRecoveredRecord 更新索引：
///     - kPut 记录：在索引中写入/覆盖该 key 的最新位置。
///     - kDelete 记录：从索引中移除该 key。
///  5. 若读取失败且为尾部截断类错误（IOError/OutOfRange），
///     调用 HandleRecoverReadFailure 截断文件并停止扫描当前 segment，
///     然后继续处理下一个 segment（跳过部分损坏的尾部）。
///  6. 不可恢复错误（Corruption/ChecksumFailed）直接向上返回。
///
/// 重放是幂等的：多次扫描同一 segment 会得到相同的索引状态。
///
/// @return 成功返回 Status::OK()，遇到不可恢复错误返回对应 Status。
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

            s = ApplyRecoveredRecord(file_id, offset, record_size, std::move(record), &index_);
            if (!s.ok()) {
                return s;
            }
            offset += record_size;
        }
    }

    return Status::OK();
}
