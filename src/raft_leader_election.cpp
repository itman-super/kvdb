#include "raft_leader_election.h"

#include <algorithm>
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
}

void LeaderElection::Start(uint64_t now_ms) {
    state_ = NodeState::kFollower;
    leader_id_.reset();
    voted_for_.reset();
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

    if (granted_votes_.size() > config_.cluster_size / 2) {
        BecomeLeader(now_ms);
    }

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

    const bool can_vote_for_candidate = !voted_for_.has_value() || voted_for_.value() == request.candidate_id;
    if (can_vote_for_candidate && IsLogUpToDate(request.last_log_index, request.last_log_term)) {
        voted_for_ = request.candidate_id;
        leader_id_.reset();
        ResetElectionDeadline(now_ms);
        response.vote_granted = true;
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
    if (granted_votes_.size() > config_.cluster_size / 2) {
        BecomeLeader(now_ms);
        return true;
    }
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
}

void LeaderElection::BecomeLeader(uint64_t now_ms) {
    state_ = NodeState::kLeader;
    leader_id_ = config_.node_id;
    last_heartbeat_sent_ms_ = now_ms;
    granted_votes_.clear();
}

void LeaderElection::ResetElectionDeadline(uint64_t now_ms) {
    election_deadline_ms_ = now_ms + RandomizedElectionTimeout();
}

uint64_t LeaderElection::RandomizedElectionTimeout() {
    std::uniform_int_distribution<uint64_t> dist(config_.min_election_timeout_ms,
                                                  config_.max_election_timeout_ms);
    return dist(rng_);
}
