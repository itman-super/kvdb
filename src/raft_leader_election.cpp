// src/raft_leader_election.cpp
//
// Raft 领导者选举状态机实现：
//  - 基于随机选举超时触发 Candidate 竞选；
//  - 处理 RequestVote / AppendEntries（心跳）；
//  - 维护 current_term、voted_for、leader_id 等关键状态；
//  - 可选落盘持久化，支持重启后恢复。
#include "raft_leader_election.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>

/// @brief 构造选举状态机并校验配置合法性，随后尝试加载持久化状态。
LeaderElection::LeaderElection(Config config) : config_(config), rng_(config.random_seed) {
    if (config_.cluster_size < 1) {
        throw std::invalid_argument("cluster_size must be >= 1");
    }
    if (config_.min_election_timeout_ms == 0 ||
        config_.max_election_timeout_ms < config_.min_election_timeout_ms) {
        throw std::invalid_argument("invalid election timeout range");
    }
    if (config_.heartbeat_interval_ms == 0) {
        throw std::invalid_argument("heartbeat_interval_ms must be > 0");
    }
    LoadPersistentState();
}

/// @brief 启动状态机：恢复或重置运行态，并初始化选举截止时间。
void LeaderElection::Start(uint64_t now_ms) {
    if (!config_.state_file_path.empty()) {
        LoadPersistentState();
    } else {
        state_ = NodeState::kFollower;
        leader_id_.reset();
        voted_for_.reset();
    }
    granted_votes_.clear();
    ResetElectionDeadline(now_ms);
    last_heartbeat_sent_ms_ = now_ms;
}

/// @brief 推进状态机时钟，判断是否触发新一轮选举或发送心跳。
LeaderElection::TickAction LeaderElection::Tick(uint64_t now_ms) {
    if (state_ == NodeState::kLeader) {
        if (now_ms >= last_heartbeat_sent_ms_ + config_.heartbeat_interval_ms) {
            last_heartbeat_sent_ms_ = now_ms;
            return TickAction::kSendHeartbeat;
        }
        return TickAction::kNone;
    }

    if (now_ms < election_deadline_ms_) {
        return TickAction::kNone;
    }

    state_ = NodeState::kCandidate;
    leader_id_.reset();
    ++current_term_;
    voted_for_ = config_.node_id;
    granted_votes_.clear();
    granted_votes_.insert(config_.node_id);
    ResetElectionDeadline(now_ms);

    if (HasMajority(granted_votes_.size())) {
        BecomeLeader(now_ms);
        return TickAction::kSendHeartbeat;
    }

    SavePersistentState();
    return TickAction::kStartElection;
}

/// @brief 生成当前节点用于拉票的 RequestVote 请求。
LeaderElection::RequestVoteRequest LeaderElection::BuildRequestVoteRequest() const {
    RequestVoteRequest request;
    request.term = current_term_;
    request.candidate_id = config_.node_id;
    request.last_log_index = last_log_index_;
    request.last_log_term = last_log_term_;
    return request;
}

/// @brief 处理来自候选者的投票请求，按任期和日志新旧规则决定是否投票。
LeaderElection::RequestVoteResponse LeaderElection::HandleRequestVote(const RequestVoteRequest& request,
                                                                      uint64_t now_ms) {
    RequestVoteResponse response;
    response.term = current_term_;
    response.vote_granted = false;

    if (request.term < current_term_) {
        return response;
    }

    if (request.term > current_term_) {
        BecomeFollower(request.term, std::nullopt, now_ms);
    }

    response.term = current_term_;

    if (request.candidate_id == 0) {
        return response;
    }

    const bool can_vote_for_candidate = !voted_for_.has_value() || voted_for_.value() == request.candidate_id;
    if (can_vote_for_candidate && IsLogUpToDate(request.last_log_index, request.last_log_term)) {
        voted_for_ = request.candidate_id;
        leader_id_.reset();
        ResetElectionDeadline(now_ms);
        response.vote_granted = true;
        SavePersistentState();
    }

    return response;
}

/// @brief 处理投票响应；若达到多数派则切换为 Leader。
bool LeaderElection::HandleRequestVoteResponse(uint32_t voter_id,
                                               const RequestVoteResponse& response,
                                               uint64_t now_ms) {
    if (response.term > current_term_) {
        BecomeFollower(response.term, std::nullopt, now_ms);
        return false;
    }

    if (state_ != NodeState::kCandidate || response.term != current_term_ || !response.vote_granted) {
        return false;
    }

    granted_votes_.insert(voter_id);
    if (HasMajority(granted_votes_.size())) {
        BecomeLeader(now_ms);
        return true;
    }
    SavePersistentState();
    return false;
}

/// @brief 处理来自 Leader 的心跳请求；若任期合法则退回 Follower 并确认成功。
LeaderElection::AppendEntriesResponse LeaderElection::HandleAppendEntries(
    const AppendEntriesRequest& request,
    uint64_t now_ms) {
    AppendEntriesResponse response;
    response.term = current_term_;
    response.success = false;

    if (request.term < current_term_) {
        return response;
    }

    BecomeFollower(request.term, request.leader_id, now_ms);
    response.term = current_term_;
    response.success = true;
    return response;
}

/// @brief 更新本地日志末尾元信息，并持久化到状态文件（若启用）。
void LeaderElection::UpdateLastLog(uint64_t last_log_index, uint64_t last_log_term) {
    last_log_index_ = last_log_index;
    last_log_term_ = last_log_term;
    SavePersistentState();
}

/// @brief 返回当前节点角色状态。
LeaderElection::NodeState LeaderElection::state() const {
    return state_;
}

/// @brief 返回当前任期号。
uint64_t LeaderElection::current_term() const {
    return current_term_;
}

/// @brief 返回当前已知 leader 节点 ID（若存在）。
std::optional<uint32_t> LeaderElection::leader_id() const {
    return leader_id_;
}

/// @brief 返回当前任期已投票的候选者 ID（若存在）。
std::optional<uint32_t> LeaderElection::voted_for() const {
    return voted_for_;
}


/// @brief 判断给定票数是否超过半数门槛。
bool LeaderElection::HasMajority(size_t vote_count) const {
    return vote_count > config_.cluster_size / 2;
}

/// @brief 按 Raft 规则比较候选者日志是否“至少一样新”。
bool LeaderElection::IsLogUpToDate(uint64_t candidate_last_log_index, uint64_t candidate_last_log_term) const {
    if (candidate_last_log_term != last_log_term_) {
        return candidate_last_log_term > last_log_term_;
    }
    return candidate_last_log_index >= last_log_index_;
}

/// @brief 切换到 Follower，并在必要时更新任期、清理投票状态、刷新超时。
void LeaderElection::BecomeFollower(uint64_t new_term,
                                    std::optional<uint32_t> known_leader,
                                    uint64_t now_ms) {
    if (new_term > current_term_) {
        current_term_ = new_term;
        voted_for_.reset();
    }
    state_ = NodeState::kFollower;
    leader_id_ = known_leader;
    granted_votes_.clear();
    ResetElectionDeadline(now_ms);
    SavePersistentState();
}

/// @brief 切换到 Leader，记录自身 leader_id 并重置心跳计时。
void LeaderElection::BecomeLeader(uint64_t now_ms) {
    state_ = NodeState::kLeader;
    leader_id_ = config_.node_id;
    last_heartbeat_sent_ms_ = now_ms;
    granted_votes_.clear();
    SavePersistentState();
}

/// @brief 按当前时间重设下一次选举超时时刻。
void LeaderElection::ResetElectionDeadline(uint64_t now_ms) {
    election_deadline_ms_ = now_ms + RandomizedElectionTimeout();
}

/// @brief 在配置区间 [min, max] 内生成随机选举超时值。
uint64_t LeaderElection::RandomizedElectionTimeout() {
    std::uniform_int_distribution<uint64_t> dist(config_.min_election_timeout_ms,
                                                  config_.max_election_timeout_ms);
    return dist(rng_);
}

/// @brief 从状态文件加载持久化状态；读取失败时保持当前内存状态。
void LeaderElection::LoadPersistentState() {
    if (config_.state_file_path.empty()) {
        return;
    }

    std::ifstream in(config_.state_file_path);
    if (!in.good()) {
        return;
    }

    uint32_t persisted_state = 0;
    uint64_t term = 0;
    int64_t voted_for = -1;
    int64_t leader_id = -1;
    uint64_t last_log_index = 0;
    uint64_t last_log_term = 0;
    if (!(in >> persisted_state >> term >> voted_for >> leader_id >> last_log_index >> last_log_term)) {
        return;
    }
    if (persisted_state > 2) {
        return;
    }

    state_ = static_cast<NodeState>(persisted_state);
    current_term_ = term;
    voted_for_ = voted_for > 0 ? std::optional<uint32_t>(static_cast<uint32_t>(voted_for)) : std::nullopt;
    leader_id_ = leader_id > 0 ? std::optional<uint32_t>(static_cast<uint32_t>(leader_id)) : std::nullopt;
    last_log_index_ = last_log_index;
    last_log_term_ = last_log_term;
}

/// @brief 将关键状态持久化到文件（覆盖写），供进程重启后恢复。
void LeaderElection::SavePersistentState() const {
    if (config_.state_file_path.empty()) {
        return;
    }

    const std::filesystem::path path(config_.state_file_path);
    if (path.has_parent_path()) {
        std::error_code ec;
        std::filesystem::create_directories(path.parent_path(), ec);
    }

    std::ofstream out(config_.state_file_path, std::ios::trunc);
    if (!out.good()) {
        return;
    }
    out << static_cast<uint32_t>(state_) << ' ' << current_term_ << ' '
        << (voted_for_.has_value() ? static_cast<int64_t>(voted_for_.value()) : -1) << ' '
        << (leader_id_.has_value() ? static_cast<int64_t>(leader_id_.value()) : -1) << ' '
        << last_log_index_ << ' ' << last_log_term_ << '\n';
}
