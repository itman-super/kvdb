#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_file.h"
#include "index_entry.h"
#include "log_record.h"
#include "options.h"
#include "status.h"

/// @brief 基于 Bitcask 思路实现的轻量级键值存储主类。
///
/// KVStore 通过 append-only 日志 + 内存索引提供高效写入与点查：
///  - 写入：顺序追加到活跃 segment；
///  - 读取：由内存索引直接定位 offset，再随机读取磁盘；
///  - 恢复：启动时从 snapshot/hint/日志重建索引。
class KVStore {
public:
    /// @brief 批量写入中的单条操作描述。
    struct WriteBatchOp {
        /// @brief 操作类型（kPut 或 kDelete）。
        RecordType type = RecordType::kPut;
        /// @brief 键，不能为空。
        std::string key;
        /// @brief 值（Delete 操作时通常为空）。
        std::string value;
    };

    /// @brief 读取一致性级别。
    enum class ReadConsistency {
        kReadCommitted,  ///< 默认级别：读取已提交（已应用）数据。
        kLinearizable,   ///< 线性一致：读取前等待已排队写入全部应用完成。
    };

    /// @brief 只读迭代器，遍历由 NewIterator() 生成的键值快照。
    class Iterator {
    public:
        /// @brief 使用已准备好的 items 快照构造迭代器。
        explicit Iterator(std::vector<std::pair<std::string, std::string>> items);
        /// @brief 当前是否指向有效元素。
        bool Valid() const;
        /// @brief 移动到下一个元素；若已无效则保持不变。
        void Next();
        /// @brief 返回当前元素 key；无效时抛出 std::out_of_range。
        const std::string& Key() const;
        /// @brief 返回当前元素 value；无效时抛出 std::out_of_range。
        const std::string& Value() const;

    private:
        std::vector<std::pair<std::string, std::string>> items_;
        std::size_t index_ = 0;
    };

    /// @brief 构造 KVStore 实例，仅保存配置，不会自动 Open。
    explicit KVStore(Options options);
    /// @brief 析构时自动停止后台线程并关闭数据库。
    ~KVStore();

    /// @brief 打开数据库目录并完成文件与索引初始化。
    Status Open();
    /// @brief 关闭数据库并落盘索引快照。
    Status Close();

    /// @brief 写入或覆盖 key。
    Status Put(const std::string& key, const std::string& value);
    /// @brief 读取 key 对应的 value。
    Status Get(const std::string& key, std::string* value);
    /// @brief 按指定一致性级别读取 key。
    Status GetWithConsistency(const std::string& key,
                              std::string* value,
                              ReadConsistency consistency);
    /// @brief 删除 key（写入 tombstone 记录）。
    Status Delete(const std::string& key);

    /// @brief 执行批量写入（按顺序处理 Put/Delete）。
    Status WriteBatch(const std::vector<WriteBatchOp>& ops);

    /// @brief 创建按 key 升序的只读迭代器快照。
    Status NewIterator(std::unique_ptr<Iterator>* iter);
    /// @brief 按 key 前缀扫描，返回升序结果。
    Status Scan(const std::string& prefix,
                std::size_t limit,
                std::vector<std::pair<std::string, std::string>>* result);
    /// @brief 按 key 升序遍历全部键值并执行回调。
    Status Fold(const std::function<Status(const std::string&, const std::string&)>& fn);

    /// @brief 执行压缩合并（Merge/Compaction）。
    Status Merge();

private:
    /// @brief 写入工作线程处理的内部请求对象。
    struct WriteRequest {
        /// @brief 请求中的批量操作集合。
        std::vector<WriteBatchOp> ops;
        /// @brief 用于异步返回处理结果。
        std::promise<Status> done;
        /// @brief 全局递增序列号，用于线性一致读取等待。
        uint64_t sequence = 0;
    };

    /// @brief 启动后台维护线程（定时 Sync/Merge）。
    void StartBackgroundWorker();
    /// @brief 停止后台维护线程。
    void StopBackgroundWorker();
    /// @brief 后台维护线程主循环。
    void BackgroundWorkerLoop();

    /// @brief 启动串行写线程。
    void StartWriteWorker();
    /// @brief 停止串行写线程。
    void StopWriteWorker();
    /// @brief 写线程主循环，按队列顺序处理写请求。
    void WriteWorkerLoop();
    /// @brief 执行单个写请求并更新索引。
    Status ProcessWriteRequest(const WriteRequest& request);
    /// @brief 将写请求入队并等待处理结果。
    Status EnqueueWriteRequest(std::vector<WriteBatchOp> ops);
    /// @brief 等待指定 sequence 及之前写入均已应用。
    void WaitForAppliedSequence(uint64_t sequence);

    /// @brief 在已持有外层互斥条件下执行 Merge 的内部实现。
    Status MergeUnlocked();

    /// @brief 从日志（或 hint）重建内存索引。
    Status Recover();

    /// @brief 追加单条日志并输出索引条目。
    Status AppendRecord(const LogRecord& record, IndexEntry* entry, bool sync_immediately);

    /// @brief 估算记录编码后大小（用于 rotate 判断）。
    uint32_t EstimateRecordSize(const LogRecord& record) const;

    /// @brief 按即将写入大小判断是否需要 rotate。
    Status RotateIfNeeded(uint32_t incoming_record_size);

    /// @brief 将当前活跃文件轮转为新活跃文件。
    Status RotateActiveFile();

    /// @brief 构建 data_<id>.log 文件路径。
    std::string BuildDataFilePath(uint32_t file_id) const;

    /// @brief 构建 index.snapshot 文件路径。
    std::string BuildIndexSnapshotPath() const;

    /// @brief 构建 hint_<id>.hint 文件路径。
    std::string BuildHintFilePath(uint32_t file_id) const;

    /// @brief 尝试从索引快照恢复内存索引。
    bool TryLoadIndexSnapshot();

    /// @brief 将当前索引写入快照文件。
    Status SaveIndexSnapshot() const;

    /// @brief 从 hint 文件恢复指定 segment 的索引信息。
    Status RecoverFromHintFile(uint32_t file_id);

    /// @brief 根据索引条目读取 value。
    Status ReadValueByEntry(const IndexEntry& entry, std::string* value);

private:
    Options options_;

    DataFile* active_file_ = nullptr;

    std::unordered_map<uint32_t, std::shared_ptr<DataFile>> data_files_;

    std::vector<uint32_t> ordered_file_ids_;

    std::unordered_map<std::string, IndexEntry> index_;

    uint32_t active_file_id_ = 1;

    std::atomic<bool> opened_{false};

    mutable std::shared_mutex store_mutex_;
    mutable std::shared_mutex index_mutex_;
    mutable std::shared_mutex files_mutex_;
    mutable std::mutex append_mutex_;
    mutable std::mutex state_mutex_;

    std::thread background_worker_;
    std::mutex background_mutex_;
    std::condition_variable background_cv_;
    bool stop_background_worker_ = false;

    std::thread write_worker_;
    std::mutex write_queue_mutex_;
    std::condition_variable write_queue_cv_;
    bool stop_write_worker_ = false;
    std::deque<WriteRequest> write_queue_;

    std::atomic<uint64_t> enqueued_sequence_{0};
    std::atomic<uint64_t> applied_sequence_{0};
    std::mutex applied_sequence_mutex_;
    std::condition_variable applied_sequence_cv_;
};
