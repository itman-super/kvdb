#pragma once

#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <unordered_set>

/// @brief Raft 领导者选举状态机。
///
/// 该模块实现 Follower/Candidate/Leader 三态切换、超时触发选举、
/// RequestVote 与心跳（AppendEntries）处理，以及最小化持久化状态保存。
class LeaderElection {
public:
    /// @brief 节点角色状态。
    enum class NodeState {
        kFollower,   ///< 跟随者：等待心跳或投票请求。
        kCandidate,  ///< 候选者：发起并等待选举投票结果。
        kLeader,     ///< 领导者：周期性发送心跳维持领导权。
    };

    /// @brief 选举模块配置项。
    struct Config {
        /// @brief 本节点 ID（需 > 0）。
        uint32_t node_id = 1;
        /// @brief 集群总节点数（用于多数派判断）。
        uint32_t cluster_size = 3;
        /// @brief 选举超时下限（毫秒）。
        uint64_t min_election_timeout_ms = 150;
        /// @brief 选举超时上限（毫秒）。
        uint64_t max_election_timeout_ms = 300;
        /// @brief Leader 心跳间隔（毫秒）。
        uint64_t heartbeat_interval_ms = 50;
        /// @brief 随机数种子，用于超时抖动。
        uint64_t random_seed = 0;
        /// @brief 持久化状态文件路径，空字符串表示不持久化。
        std::string state_file_path;
    };

    /// @brief RequestVote RPC 请求。
    struct RequestVoteRequest {
        uint64_t term = 0;
        uint32_t candidate_id = 0;
        uint64_t last_log_index = 0;
        uint64_t last_log_term = 0;
    };

    /// @brief RequestVote RPC 响应。
    struct RequestVoteResponse {
        uint64_t term = 0;
        bool vote_granted = false;
    };

    /// @brief AppendEntries（心跳）RPC 请求。
    struct AppendEntriesRequest {
        uint64_t term = 0;
        uint32_t leader_id = 0;
    };

    /// @brief AppendEntries（心跳）RPC 响应。
    struct AppendEntriesResponse {
        uint64_t term = 0;
        bool success = false;
    };

    /// @brief 每次 Tick 后调用方应执行的动作建议。
    enum class TickAction {
        kNone,           ///< 当前无需发送网络消息。
        kStartElection,  ///< 需要发起 RequestVote。
        kSendHeartbeat,  ///< 需要发送心跳或首次 leader 广播。
    };

    /// @brief 构造状态机并校验配置合法性。
    explicit LeaderElection(Config config);

    /// @brief 启动状态机，初始化角色与超时截止时间。
    void Start(uint64_t now_ms);
    /// @brief 推进状态机时钟，可能触发选举或心跳动作。
    TickAction Tick(uint64_t now_ms);

    /// @brief 构造当前任期下的 RequestVote 请求。
    RequestVoteRequest BuildRequestVoteRequest() const;

    /// @brief 处理来自其他节点的 RequestVote 请求。
    RequestVoteResponse HandleRequestVote(const RequestVoteRequest& request, uint64_t now_ms);
    /// @brief 处理对本节点候选请求的投票响应。
    /// @return 若因此成为 Leader 返回 true，否则 false。
    bool HandleRequestVoteResponse(uint32_t voter_id,
                                   const RequestVoteResponse& response,
                                   uint64_t now_ms);

    /// @brief 处理来自 Leader 的 AppendEntries（心跳）请求。
    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request, uint64_t now_ms);

    /// @brief 更新本地日志末尾索引/任期，用于投票日志新旧比较。
    void UpdateLastLog(uint64_t last_log_index, uint64_t last_log_term);

    /// @brief 读取当前节点状态。
    NodeState state() const;
    /// @brief 读取当前任期。
    uint64_t current_term() const;
    /// @brief 读取当前已知 leader（若存在）。
    std::optional<uint32_t> leader_id() const;
    /// @brief 读取当前任期已投票对象（若存在）。
    std::optional<uint32_t> voted_for() const;

private:
    /// @brief 判断给定票数是否达到多数派。
    bool HasMajority(size_t vote_count) const;
    /// @brief 判断候选者日志是否至少与本地一样新。
    bool IsLogUpToDate(uint64_t candidate_last_log_index, uint64_t candidate_last_log_term) const;
    /// @brief 切换到 Follower 状态并重置选举相关状态。
    void BecomeFollower(uint64_t new_term, std::optional<uint32_t> known_leader, uint64_t now_ms);
    /// @brief 切换到 Leader 状态并重置心跳计时。
    void BecomeLeader(uint64_t now_ms);
    /// @brief 重置下一次选举超时截止点。
    void ResetElectionDeadline(uint64_t now_ms);
    /// @brief 生成随机选举超时（位于配置区间内）。
    uint64_t RandomizedElectionTimeout();
    /// @brief 从状态文件读取持久化信息。
    void LoadPersistentState();
    /// @brief 将当前关键状态写入状态文件。
    void SavePersistentState() const;

private:
    Config config_;
    NodeState state_ = NodeState::kFollower;
    uint64_t current_term_ = 0;
    std::optional<uint32_t> voted_for_;
    std::optional<uint32_t> leader_id_;

    uint64_t last_log_index_ = 0;
    uint64_t last_log_term_ = 0;

    uint64_t election_deadline_ms_ = 0;
    uint64_t last_heartbeat_sent_ms_ = 0;

    std::unordered_set<uint32_t> granted_votes_;
    std::mt19937_64 rng_;
};
