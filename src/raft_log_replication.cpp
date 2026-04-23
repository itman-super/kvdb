// src/raft_log_replication.cpp
//
// Raft 日志复制模块实现：
//  - Leader 侧：构建 AppendEntries、处理响应、推进 commitIndex；
//  - Follower 侧：校验 prevLog 并执行冲突回退与日志覆盖；
//  - 快照：支持创建/安装快照以及基于 nextIndex 的快照下发判断；
//  - 可选持久化：将日志、快照和复制状态保存到本地文件。
#include "raft_log_replication.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>

/// @brief 构造复制状态机并按需加载持久化状态。
RaftLogReplication::RaftLogReplication(uint32_t node_id, std::string state_file_path)
    : node_id_(node_id), state_file_path_(std::move(state_file_path)) {
    if (node_id_ == 0) {
        throw std::invalid_argument("node_id must be > 0");
    }
    if (!state_file_path_.empty()) {
        LoadPersistentState();
    } else {
        Reset();
    }
}

/// @brief 将状态机重置为初始状态并持久化。
void RaftLogReplication::Reset() {
    snapshot_ = Snapshot{};
    log_.clear();
    commit_index_ = 0;
    next_index_.clear();
    match_index_.clear();
    SavePersistentState();
}

/// @brief 返回本地最后日志索引（日志为空时回退到快照边界）。
uint64_t RaftLogReplication::last_log_index() const {
    return log_.empty() ? snapshot_.last_included_index : log_.back().index;
}

/// @brief 返回本地最后日志任期（日志为空时回退到快照边界）。
uint64_t RaftLogReplication::last_log_term() const {
    return log_.empty() ? snapshot_.last_included_term : log_.back().term;
}

/// @brief 返回当前提交索引。
uint64_t RaftLogReplication::commit_index() const {
    return commit_index_;
}

/// @brief 判断候选者日志是否不落后于本地日志。
bool RaftLogReplication::IsLogUpToDate(uint64_t candidate_last_log_index,
                                       uint64_t candidate_last_log_term) const {
    const uint64_t local_last_term = last_log_term();
    if (candidate_last_log_term != local_last_term) {
        return candidate_last_log_term > local_last_term;
    }
    return candidate_last_log_index >= last_log_index();
}

/// @brief Leader 在本地追加一条新日志并返回其索引。
uint64_t RaftLogReplication::AppendLocalEntry(uint64_t current_term, std::string command) {
    const uint64_t next = last_log_index() + 1;
    log_.push_back(LogEntry{next, current_term, std::move(command)});
    SavePersistentState();
    return next;
}

/// @brief Leader 初始化各 peer 的 nextIndex/matchIndex。
void RaftLogReplication::InitLeaderReplication(const std::vector<uint32_t>& peer_ids) {
    next_index_.clear();
    match_index_.clear();
    const uint64_t start_index = last_log_index() + 1;
    for (uint32_t peer_id : peer_ids) {
        if (peer_id == node_id_) {
            continue;
        }
        next_index_[peer_id] = start_index;
        match_index_[peer_id] = snapshot_.last_included_index;
    }
    SavePersistentState();
}

/// @brief 为指定 peer 构建 AppendEntries 请求，可限制最大 entries 数。
RaftLogReplication::AppendEntriesRequest RaftLogReplication::BuildAppendEntriesRequest(uint32_t peer_id,
                                                                                        uint64_t current_term,
                                                                                        uint32_t leader_id,
                                                                                        size_t max_entries) const {
    AppendEntriesRequest req;
    req.term = current_term;
    req.leader_id = leader_id;
    req.leader_commit = commit_index_;

    uint64_t next = last_log_index() + 1;
    auto it = next_index_.find(peer_id);
    if (it != next_index_.end()) {
        next = it->second;
    }

    if (next <= snapshot_.last_included_index) {
        next = snapshot_.last_included_index + 1;
    }

    req.prev_log_index = (next == 0) ? 0 : (next - 1);
    req.prev_log_term = FindTerm(req.prev_log_index);

    if (next == 0 || next > last_log_index()) {
        return req;
    }

    const size_t start = ToVectorPos(next);
    const size_t end = std::min(log_.size(), start + max_entries);
    req.entries.insert(req.entries.end(), log_.begin() + static_cast<std::ptrdiff_t>(start),
                       log_.begin() + static_cast<std::ptrdiff_t>(end));
    return req;
}

/// @brief Leader 处理 follower 响应并更新 nextIndex/matchIndex 与 commitIndex。
void RaftLogReplication::HandleAppendEntriesResponse(uint32_t peer_id,
                                                     const AppendEntriesResponse& response,
                                                     uint64_t current_term) {
    if (response.term > current_term) {
        return;
    }

    auto next_it = next_index_.find(peer_id);
    auto match_it = match_index_.find(peer_id);
    if (next_it == next_index_.end() || match_it == match_index_.end()) {
        return;
    }

    if (response.success) {
        match_it->second = std::max(match_it->second, response.match_index);
        next_it->second = std::max(next_it->second, response.match_index + 1);

        std::vector<uint64_t> matched_indexes;
        matched_indexes.reserve(match_index_.size() + 1);
        matched_indexes.push_back(last_log_index());
        for (const auto& [_, matched] : match_index_) {
            matched_indexes.push_back(matched);
        }
        std::sort(matched_indexes.begin(), matched_indexes.end());
        const uint64_t quorum_match = matched_indexes[matched_indexes.size() / 2];
        if (quorum_match > commit_index_ && FindTerm(quorum_match) == current_term) {
            commit_index_ = quorum_match;
        }
    } else if (response.conflict_index > 0) {
        next_it->second = response.conflict_index;
    } else if (next_it->second > snapshot_.last_included_index + 1) {
        --next_it->second;
    }
    SavePersistentState();
}

/// @brief Follower 处理 AppendEntries：校验前置日志、冲突覆盖并推进提交点。
RaftLogReplication::AppendEntriesResponse RaftLogReplication::HandleAppendEntries(
    const AppendEntriesRequest& request) {
    AppendEntriesResponse response;
    response.term = request.term;
    response.success = false;
    response.match_index = 0;
    response.conflict_index = last_log_index() + 1;

    if (request.prev_log_index < snapshot_.last_included_index) {
        response.conflict_index = snapshot_.last_included_index + 1;
        return response;
    }

    if (request.prev_log_index > last_log_index()) {
        return response;
    }

    if (request.prev_log_index > 0 && FindTerm(request.prev_log_index) != request.prev_log_term) {
        response.conflict_index = FindConflictIndex(request.prev_log_index);
        return response;
    }

    uint64_t insert_index = request.prev_log_index + 1;
    for (const auto& entry : request.entries) {
        if (insert_index <= last_log_index()) {
            if (FindTerm(insert_index) != entry.term) {
                log_.erase(log_.begin() + static_cast<std::ptrdiff_t>(ToVectorPos(insert_index)), log_.end());
            }
        }

        if (insert_index > last_log_index()) {
            log_.push_back(LogEntry{insert_index, entry.term, entry.command});
        }
        ++insert_index;
    }

    if (request.leader_commit > commit_index_) {
        commit_index_ = std::min(request.leader_commit, last_log_index());
    }

    SavePersistentState();
    response.success = true;
    response.match_index = (insert_index == 0) ? 0 : (insert_index - 1);
    response.conflict_index = 0;
    return response;
}

/// @brief 创建快照并裁剪已包含在快照中的日志条目。
bool RaftLogReplication::CreateSnapshot(uint64_t last_included_index, std::string snapshot_data) {
    if (last_included_index <= snapshot_.last_included_index || last_included_index > commit_index_) {
        return false;
    }

    const uint64_t snap_term = FindTerm(last_included_index);
    if (snap_term == 0) {
        return false;
    }

    log_.erase(std::remove_if(log_.begin(), log_.end(), [&](const LogEntry& entry) {
                   return entry.index <= last_included_index;
               }),
               log_.end());

    snapshot_.last_included_index = last_included_index;
    snapshot_.last_included_term = snap_term;
    snapshot_.data = std::move(snapshot_data);
    SavePersistentState();
    return true;
}

/// @brief 安装外部快照并丢弃快照点之前的本地日志。
bool RaftLogReplication::InstallSnapshot(const Snapshot& snapshot) {
    if (snapshot.last_included_index <= snapshot_.last_included_index) {
        return false;
    }

    snapshot_ = snapshot;
    log_.erase(std::remove_if(log_.begin(), log_.end(), [&](const LogEntry& entry) {
                   return entry.index <= snapshot_.last_included_index;
               }),
               log_.end());
    commit_index_ = std::max(commit_index_, snapshot_.last_included_index);
    SavePersistentState();
    return true;
}

/// @brief 判断指定 peer 的 nextIndex 是否落后到需要快照补齐。
bool RaftLogReplication::NeedsSnapshot(uint32_t peer_id) const {
    const auto it = next_index_.find(peer_id);
    if (it == next_index_.end()) {
        return false;
    }
    return it->second <= snapshot_.last_included_index;
}

/// @brief 若 peer 需要快照则返回当前快照，否则返回 nullopt。
std::optional<RaftLogReplication::Snapshot> RaftLogReplication::BuildSnapshotForPeer(uint32_t peer_id) const {
    if (!NeedsSnapshot(peer_id)) {
        return std::nullopt;
    }
    return snapshot_;
}

/// @brief 返回当前快照（若尚未创建快照则返回 nullopt）。
std::optional<RaftLogReplication::Snapshot> RaftLogReplication::snapshot() const {
    if (snapshot_.last_included_index == 0) {
        return std::nullopt;
    }
    return snapshot_;
}

/// @brief 按日志索引读取单条日志，不可读时返回 nullopt。
std::optional<RaftLogReplication::LogEntry> RaftLogReplication::GetEntry(uint64_t log_index) const {
    if (log_index == 0 || log_index <= snapshot_.last_included_index || log_index > last_log_index()) {
        return std::nullopt;
    }
    return log_[ToVectorPos(log_index)];
}

/// @brief 获取已提交但尚未应用的日志区间 (last_applied, commit_index]。
std::vector<RaftLogReplication::LogEntry> RaftLogReplication::GetCommittedEntriesSince(
    uint64_t last_applied) const {
    std::vector<LogEntry> committed;
    if (last_applied >= commit_index_) {
        return committed;
    }

    uint64_t start = std::max(last_applied + 1, snapshot_.last_included_index + 1);
    for (uint64_t i = start; i <= commit_index_; ++i) {
        committed.push_back(log_[ToVectorPos(i)]);
    }
    return committed;
}

/// @brief 将逻辑日志索引映射为 log_ 的向量下标。
size_t RaftLogReplication::ToVectorPos(uint64_t log_index) const {
    return static_cast<size_t>(log_index - snapshot_.last_included_index - 1);
}

/// @brief 找到冲突任期在本地日志中的起始索引（用于 follower 回退）。
uint64_t RaftLogReplication::FindConflictIndex(uint64_t prev_log_index) const {
    const uint64_t conflict_term = FindTerm(prev_log_index);
    uint64_t idx = prev_log_index;
    while (idx > snapshot_.last_included_index + 1 && FindTerm(idx - 1) == conflict_term) {
        --idx;
    }
    return idx;
}

/// @brief 查询指定日志索引的任期；索引不可用时返回 0。
uint64_t RaftLogReplication::FindTerm(uint64_t log_index) const {
    if (log_index == snapshot_.last_included_index) {
        return snapshot_.last_included_term;
    }
    if (log_index == 0 || log_index > last_log_index() || log_index <= snapshot_.last_included_index) {
        return 0;
    }
    return log_[ToVectorPos(log_index)].term;
}

/// @brief 从持久化文件加载快照、日志与复制状态；失败时重置状态机。
void RaftLogReplication::LoadPersistentState() {
    if (state_file_path_.empty()) {
        return;
    }

    std::ifstream in(state_file_path_);
    if (!in.good()) {
        Reset();
        return;
    }

    uint64_t persisted_snapshot_index = 0;
    uint64_t persisted_snapshot_term = 0;
    std::string persisted_snapshot_data;
    if (!(in >> persisted_snapshot_index >> persisted_snapshot_term)) {
        Reset();
        return;
    }
    in.get();
    std::getline(in, persisted_snapshot_data);

    uint64_t persisted_commit = 0;
    size_t log_size = 0;
    if (!(in >> persisted_commit >> log_size)) {
        Reset();
        return;
    }

    std::vector<LogEntry> loaded_log;
    loaded_log.reserve(log_size);
    for (size_t i = 0; i < log_size; ++i) {
        uint64_t index = 0;
        uint64_t term = 0;
        std::string command;
        if (!(in >> index >> term >> std::ws)) {
            Reset();
            return;
        }
        std::getline(in, command);
        loaded_log.push_back(LogEntry{index, term, command});
    }

    size_t next_size = 0;
    if (!(in >> next_size)) {
        Reset();
        return;
    }
    std::unordered_map<uint32_t, uint64_t> loaded_next;
    for (size_t i = 0; i < next_size; ++i) {
        uint32_t peer = 0;
        uint64_t value = 0;
        if (!(in >> peer >> value)) {
            Reset();
            return;
        }
        loaded_next[peer] = value;
    }

    size_t match_size = 0;
    if (!(in >> match_size)) {
        Reset();
        return;
    }
    std::unordered_map<uint32_t, uint64_t> loaded_match;
    for (size_t i = 0; i < match_size; ++i) {
        uint32_t peer = 0;
        uint64_t value = 0;
        if (!(in >> peer >> value)) {
            Reset();
            return;
        }
        loaded_match[peer] = value;
    }

    snapshot_.last_included_index = persisted_snapshot_index;
    snapshot_.last_included_term = persisted_snapshot_term;
    snapshot_.data = std::move(persisted_snapshot_data);
    log_ = std::move(loaded_log);
    commit_index_ = std::min(persisted_commit, last_log_index());
    next_index_ = std::move(loaded_next);
    match_index_ = std::move(loaded_match);
}

/// @brief 将快照、日志与复制状态完整写入持久化文件。
void RaftLogReplication::SavePersistentState() const {
    if (state_file_path_.empty()) {
        return;
    }

    const std::filesystem::path path(state_file_path_);
    if (path.has_parent_path()) {
        std::error_code ec;
        std::filesystem::create_directories(path.parent_path(), ec);
    }

    std::ofstream out(state_file_path_, std::ios::trunc);
    if (!out.good()) {
        return;
    }

    out << snapshot_.last_included_index << ' ' << snapshot_.last_included_term << ' ' << snapshot_.data
        << '\n';
    out << commit_index_ << ' ' << log_.size() << '\n';
    for (const auto& entry : log_) {
        out << entry.index << ' ' << entry.term << ' ' << entry.command << '\n';
    }
    out << next_index_.size() << '\n';
    for (const auto& [peer, next] : next_index_) {
        out << peer << ' ' << next << '\n';
    }
    out << match_index_.size() << '\n';
    for (const auto& [peer, match] : match_index_) {
        out << peer << ' ' << match << '\n';
    }
}
