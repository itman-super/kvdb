#include "raft_log_replication.h"

#include <algorithm>
#include <stdexcept>

RaftLogReplication::RaftLogReplication(uint32_t node_id) : node_id_(node_id) {
    if (node_id_ == 0) {
        throw std::invalid_argument("node_id must be > 0");
    }
    Reset();
}

void RaftLogReplication::Reset() {
    log_.clear();
    commit_index_ = 0;
    next_index_.clear();
    match_index_.clear();
}

uint64_t RaftLogReplication::last_log_index() const {
    return log_.empty() ? 0 : log_.back().index;
}

uint64_t RaftLogReplication::last_log_term() const {
    return log_.empty() ? 0 : log_.back().term;
}

uint64_t RaftLogReplication::commit_index() const {
    return commit_index_;
}

bool RaftLogReplication::IsLogUpToDate(uint64_t candidate_last_log_index,
                                       uint64_t candidate_last_log_term) const {
    const uint64_t local_last_term = last_log_term();
    if (candidate_last_log_term != local_last_term) {
        return candidate_last_log_term > local_last_term;
    }
    return candidate_last_log_index >= last_log_index();
}

uint64_t RaftLogReplication::AppendLocalEntry(uint64_t current_term, std::string command) {
    const uint64_t next = last_log_index() + 1;
    log_.push_back(LogEntry{next, current_term, std::move(command)});
    return next;
}

void RaftLogReplication::InitLeaderReplication(const std::vector<uint32_t>& peer_ids) {
    next_index_.clear();
    match_index_.clear();
    const uint64_t start_index = last_log_index() + 1;
    for (uint32_t peer_id : peer_ids) {
        if (peer_id == node_id_) {
            continue;
        }
        next_index_[peer_id] = start_index;
        match_index_[peer_id] = 0;
    }
}

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
    } else if (next_it->second > 1) {
        --next_it->second;
    }
}

RaftLogReplication::AppendEntriesResponse RaftLogReplication::HandleAppendEntries(
    const AppendEntriesRequest& request) {
    AppendEntriesResponse response;
    response.term = request.term;
    response.success = false;
    response.match_index = 0;
    response.conflict_index = last_log_index() + 1;

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

    response.success = true;
    response.match_index = (insert_index == 0) ? 0 : (insert_index - 1);
    response.conflict_index = 0;
    return response;
}

std::optional<RaftLogReplication::LogEntry> RaftLogReplication::GetEntry(uint64_t log_index) const {
    if (log_index == 0 || log_index > last_log_index()) {
        return std::nullopt;
    }
    return log_[ToVectorPos(log_index)];
}

std::vector<RaftLogReplication::LogEntry> RaftLogReplication::GetCommittedEntriesSince(
    uint64_t last_applied) const {
    std::vector<LogEntry> committed;
    if (last_applied >= commit_index_) {
        return committed;
    }

    for (uint64_t i = last_applied + 1; i <= commit_index_; ++i) {
        committed.push_back(log_[ToVectorPos(i)]);
    }
    return committed;
}

size_t RaftLogReplication::ToVectorPos(uint64_t log_index) const {
    return static_cast<size_t>(log_index - 1);
}

uint64_t RaftLogReplication::FindConflictIndex(uint64_t prev_log_index) const {
    const uint64_t conflict_term = FindTerm(prev_log_index);
    uint64_t idx = prev_log_index;
    while (idx > 1 && FindTerm(idx - 1) == conflict_term) {
        --idx;
    }
    return idx;
}

uint64_t RaftLogReplication::FindTerm(uint64_t log_index) const {
    if (log_index == 0 || log_index > last_log_index()) {
        return 0;
    }
    return log_[ToVectorPos(log_index)].term;
}
