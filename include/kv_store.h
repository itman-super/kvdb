#pragma once

#include <functional>
#include <condition_variable>
#include <mutex>
#include <memory>
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

// KVStore 是对外暴露的数据库主类。
// 当前实现支持多个 segment(data_x.log)：
// - 启动时扫描目录内全部 segment 并恢复索引
// - 写入达到阈值后自动 rotate 到新 segment
// - 读取会按 index 中的 file_id 定位到对应文件
class KVStore {
public:
    /// @brief 批处理操作描述：支持 put/delete 混合。
    struct WriteBatchOp {
        RecordType type = RecordType::kPut;
        std::string key;
        std::string value;
    };

    /// @brief 基于快照的只读迭代器（按 key 升序）。
    class Iterator {
    public:
        /// @brief 构造快照迭代器。
        explicit Iterator(std::vector<std::pair<std::string, std::string>> items);
        /// @brief 当前是否仍指向有效元素。
        bool Valid() const;
        /// @brief 前进到下一项。
        void Next();
        /// @brief 获取当前 key；若无效会抛出 out_of_range。
        const std::string& Key() const;
        /// @brief 获取当前 value；若无效会抛出 out_of_range。
        const std::string& Value() const;

    private:
        std::vector<std::pair<std::string, std::string>> items_;
        std::size_t index_ = 0;
    };

    /// @brief 构造数据库对象（不自动打开）。
    explicit KVStore(Options options);

    /// @brief 析构时自动关闭并清理后台线程。
    ~KVStore();

    /// @brief 打开数据库并执行恢复流程。
    Status Open();

    /// @brief 关闭数据库并刷新必要元数据。
    Status Close();

    /// @brief 写入或覆盖一个 key。
    Status Put(const std::string& key, const std::string& value);

    /// @brief 读取 key 对应的 value。
    Status Get(const std::string& key, std::string* value);

    /// @brief 删除 key（通过写入 tombstone 实现）。
    Status Delete(const std::string& key);

    /// @brief 顺序执行批量写入操作。
    Status WriteBatch(const std::vector<WriteBatchOp>& ops);

    /// @brief 创建按 key 升序的快照迭代器。
    Status NewIterator(std::unique_ptr<Iterator>* iter);

    /// @brief 按前缀扫描 key（升序），limit=0 表示不限制返回条数。
    Status Scan(const std::string& prefix,
                std::size_t limit,
                std::vector<std::pair<std::string, std::string>>* result);

    /// @brief 按 key 升序遍历全部键值并执行回调。
    Status Fold(const std::function<Status(const std::string&, const std::string&)>& fn);

    /// @brief 手动触发 Merge/Compaction，仅保留存活 key 的最新值。
    Status Merge();

private:
    // 启动后台任务线程（定期 sync/merge）。
    void StartBackgroundWorker();

    // 停止后台任务线程。
    void StopBackgroundWorker();

    // 后台任务主循环。
    void BackgroundWorkerLoop();

    // 在持有 store_mutex_ 独占锁时执行一次 merge。
    Status MergeUnlocked();

    // 启动恢复：按 file_id 升序扫描所有 segment，重建 index_。
    Status Recover();

    // 统一追加日志记录，并回填 IndexEntry。
    Status AppendRecord(const LogRecord& record, IndexEntry* entry);

    // 根据 key/value 大小估算本条记录编码后占用字节数。
    uint32_t EstimateRecordSize(const LogRecord& record) const;

    // 如果 active segment 超过阈值则执行 rotate。
    Status RotateIfNeeded(uint32_t incoming_record_size);

    // 创建并切换到下一个 active segment。
    Status RotateActiveFile();

    // 生成指定 file_id 的 segment 文件路径。
    std::string BuildDataFilePath(uint32_t file_id) const;

    // 生成 index snapshot 文件路径。
    std::string BuildIndexSnapshotPath() const;

    // 生成指定 file_id 的 hint 文件路径。
    std::string BuildHintFilePath(uint32_t file_id) const;

    // 尝试加载 index snapshot；成功返回 true，失败返回 false（不视为错误）。
    bool TryLoadIndexSnapshot();

    // 将当前内存索引持久化为 snapshot。
    Status SaveIndexSnapshot() const;

    // 从 hint 文件恢复某个 segment 的索引记录。
    // 仅适用于只包含 put 记录的 compacted segment。
    Status RecoverFromHintFile(uint32_t file_id);

    // 根据索引读取 value。
    Status ReadValueByEntry(const IndexEntry& entry, std::string* value);

private:
    Options options_;

    // 当前活跃数据文件。
    DataFile* active_file_ = nullptr;

    // 所有已打开数据文件：file_id -> DataFile。
    std::unordered_map<uint32_t, std::unique_ptr<DataFile>> data_files_;

    // 已知文件号的有序视图（用于恢复时顺序扫描）。
    std::vector<uint32_t> ordered_file_ids_;

    // 内存索引：key -> 最新记录位置。
    std::unordered_map<std::string, IndexEntry> index_;

    // 当前 active file 的文件号。
    uint32_t active_file_id_ = 1;

    // 数据库是否已打开。
    bool opened_ = false;

    // 读写锁：读路径共享锁，写路径独占锁。
    mutable std::shared_mutex store_mutex_;

    // 后台线程控制。
    std::thread background_worker_;
    std::mutex background_mutex_;
    std::condition_variable background_cv_;
    bool stop_background_worker_ = false;
};
