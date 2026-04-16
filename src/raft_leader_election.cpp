#include "raft_leader_election.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>

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

LeaderElection::RequestVoteRequest LeaderElection::BuildRequestVoteRequest() const {
    RequestVoteRequest request;
    request.term = current_term_;
    request.candidate_id = config_.node_id;
    request.last_log_index = last_log_index_;
    request.last_log_term = last_log_term_;
    return request;
}

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

void LeaderElection::UpdateLastLog(uint64_t last_log_index, uint64_t last_log_term) {
    last_log_index_ = last_log_index;
    last_log_term_ = last_log_term;
    SavePersistentState();
}

LeaderElection::NodeState LeaderElection::state() const {
    return state_;
}

uint64_t LeaderElection::current_term() const {
    return current_term_;
}

std::optional<uint32_t> LeaderElection::leader_id() const {
    return leader_id_;
}

std::optional<uint32_t> LeaderElection::voted_for() const {
    return voted_for_;
}


bool LeaderElection::HasMajority(size_t vote_count) const {
    return vote_count > config_.cluster_size / 2;
}

bool LeaderElection::IsLogUpToDate(uint64_t candidate_last_log_index, uint64_t candidate_last_log_term) const {
    if (candidate_last_log_term != last_log_term_) {
        return candidate_last_log_term > last_log_term_;
    }
    return candidate_last_log_index >= last_log_index_;
}

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

void LeaderElection::BecomeLeader(uint64_t now_ms) {
    state_ = NodeState::kLeader;
    leader_id_ = config_.node_id;
    last_heartbeat_sent_ms_ = now_ms;
    granted_votes_.clear();
    SavePersistentState();
}

void LeaderElection::ResetElectionDeadline(uint64_t now_ms) {
    election_deadline_ms_ = now_ms + RandomizedElectionTimeout();
}

uint64_t LeaderElection::RandomizedElectionTimeout() {
    std::uniform_int_distribution<uint64_t> dist(config_.min_election_timeout_ms,
                                                  config_.max_election_timeout_ms);
    return dist(rng_);
}

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
