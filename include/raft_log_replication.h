#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

/// @brief Raft 日志复制与快照管理状态机。
///
/// 该模块维护本地日志、提交点、每个 follower 的 nextIndex/matchIndex，
/// 并提供 AppendEntries 构建与处理、冲突回退、快照创建与安装能力。
class RaftLogReplication {
public:
    /// @brief 单条 Raft 日志条目。
    struct LogEntry {
        uint64_t index = 0;
        uint64_t term = 0;
        std::string command;
    };

    /// @brief AppendEntries RPC 请求。
    struct AppendEntriesRequest {
        uint64_t term = 0;
        uint32_t leader_id = 0;
        uint64_t prev_log_index = 0;
        uint64_t prev_log_term = 0;
        std::vector<LogEntry> entries;
        uint64_t leader_commit = 0;
    };

    /// @brief AppendEntries RPC 响应。
    struct AppendEntriesResponse {
        uint64_t term = 0;
        bool success = false;
        uint64_t match_index = 0;
        uint64_t conflict_index = 0;
    };

    /// @brief Raft 快照元信息与快照数据。
    struct Snapshot {
        uint64_t last_included_index = 0;
        uint64_t last_included_term = 0;
        std::string data;
    };

    /// @brief 构造日志复制状态机。
    /// @param node_id         本节点 ID（需 > 0）。
    /// @param state_file_path 持久化状态文件路径，空字符串表示仅内存状态。
    explicit RaftLogReplication(uint32_t node_id, std::string state_file_path = "");

    /// @brief 重置状态机到初始状态，并持久化。
    void Reset();

    /// @brief 返回当前日志最后一条索引（含快照边界）。
    uint64_t last_log_index() const;
    /// @brief 返回当前日志最后一条任期（含快照边界）。
    uint64_t last_log_term() const;
    /// @brief 返回当前已提交索引。
    uint64_t commit_index() const;

    /// @brief 判断候选者日志是否至少和本地一样新。
    bool IsLogUpToDate(uint64_t candidate_last_log_index, uint64_t candidate_last_log_term) const;

    /// @brief 由 Leader 在本地追加新命令日志。
    /// @return 新日志条目的 index。
    uint64_t AppendLocalEntry(uint64_t current_term, std::string command);

    /// @brief 成为 Leader 后初始化对各 peer 的复制状态。
    void InitLeaderReplication(const std::vector<uint32_t>& peer_ids);
    /// @brief 为指定 peer 构建 AppendEntries 请求。
    AppendEntriesRequest BuildAppendEntriesRequest(uint32_t peer_id,
                                                   uint64_t current_term,
                                                   uint32_t leader_id,
                                                   size_t max_entries = 32) const;
    /// @brief Leader 处理来自 peer 的 AppendEntries 响应并推进复制状态。
    void HandleAppendEntriesResponse(uint32_t peer_id,
                                     const AppendEntriesResponse& response,
                                     uint64_t current_term);

    /// @brief Follower 处理来自 Leader 的 AppendEntries 请求。
    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request);

    /// @brief 创建快照并裁剪已纳入快照的日志。
    bool CreateSnapshot(uint64_t last_included_index, std::string snapshot_data);
    /// @brief 安装来自 Leader 的快照。
    bool InstallSnapshot(const Snapshot& snapshot);
    /// @brief 判断某个 peer 是否需要先发送快照。
    bool NeedsSnapshot(uint32_t peer_id) const;
    /// @brief 若 peer 需要快照则返回快照内容，否则返回 nullopt。
    std::optional<Snapshot> BuildSnapshotForPeer(uint32_t peer_id) const;
    /// @brief 返回当前快照（若存在）。
    std::optional<Snapshot> snapshot() const;

    /// @brief 按逻辑索引读取单条日志。
    std::optional<LogEntry> GetEntry(uint64_t log_index) const;
    /// @brief 获取 (last_applied, commit_index] 区间内已提交日志。
    std::vector<LogEntry> GetCommittedEntriesSince(uint64_t last_applied) const;

private:
    /// @brief 将逻辑日志索引映射到 log_ 向量下标。
    size_t ToVectorPos(uint64_t log_index) const;
    /// @brief 计算冲突任期的首个索引（用于 follower 回退）。
    uint64_t FindConflictIndex(uint64_t prev_log_index) const;
    /// @brief 查询给定索引对应任期（不存在返回 0）。
    uint64_t FindTerm(uint64_t log_index) const;
    /// @brief 从磁盘加载持久化状态。
    void LoadPersistentState();
    /// @brief 将当前状态写入持久化文件。
    void SavePersistentState() const;

private:
    uint32_t node_id_;
    std::string state_file_path_;
    Snapshot snapshot_;
    std::vector<LogEntry> log_;
    uint64_t commit_index_ = 0;

    std::unordered_map<uint32_t, uint64_t> next_index_;
    std::unordered_map<uint32_t, uint64_t> match_index_;
};
