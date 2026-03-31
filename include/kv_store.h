#pragma once

#include <memory>
#include <string>
#include <unordered_map>
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
    // 构造数据库对象。
    explicit KVStore(Options options);

    // 析构时自动 Close，释放文件资源。
    ~KVStore();

    // 打开数据库并完成恢复。
    // 步骤：创建目录 -> 打开/创建 segment -> 扫描日志恢复索引。
    Status Open();

    // 关闭数据库并释放句柄。
    Status Close();

    // 写入/覆盖 key。
    Status Put(const std::string& key, const std::string& value);

    // 读取 key 对应 value。
    Status Get(const std::string& key, std::string* value);

    // 删除 key（写入 tombstone）。
    Status Delete(const std::string& key);

    // 手动触发 Merge/Compaction：
    // - 仅保留当前“存活”的 key 最新值
    // - 清理历史旧版本和 tombstone
    // - 产出新的单一 active segment
    Status Merge();

private:
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
};
