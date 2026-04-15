#pragma once

#include <cstdint>
#include <optional>
#include <random>
#include <unordered_set>

class LeaderElection {
public:
    enum class NodeState {
        kFollower,
        kCandidate,
        kLeader,
    };

    struct Config {
        uint32_t node_id = 1;
        uint32_t cluster_size = 3;
        uint64_t min_election_timeout_ms = 150;
        uint64_t max_election_timeout_ms = 300;
        uint64_t heartbeat_interval_ms = 50;
        uint64_t random_seed = 0;
    };

    struct RequestVoteRequest {
        uint64_t term = 0;
        uint32_t candidate_id = 0;
        uint64_t last_log_index = 0;
        uint64_t last_log_term = 0;
    };

    struct RequestVoteResponse {
        uint64_t term = 0;
        bool vote_granted = false;
    };

    struct AppendEntriesRequest {
        uint64_t term = 0;
        uint32_t leader_id = 0;
    };

    struct AppendEntriesResponse {
        uint64_t term = 0;
        bool success = false;
    };

    enum class TickAction {
        kNone,
        kStartElection,
        kSendHeartbeat,
    };

    explicit LeaderElection(Config config);

    void Start(uint64_t now_ms);
    TickAction Tick(uint64_t now_ms);

    RequestVoteRequest BuildRequestVoteRequest() const;

    RequestVoteResponse HandleRequestVote(const RequestVoteRequest& request, uint64_t now_ms);
    bool HandleRequestVoteResponse(uint32_t voter_id,
                                   const RequestVoteResponse& response,
                                   uint64_t now_ms);

    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request, uint64_t now_ms);

    void UpdateLastLog(uint64_t last_log_index, uint64_t last_log_term);

    NodeState state() const;
    uint64_t current_term() const;
    std::optional<uint32_t> leader_id() const;
    std::optional<uint32_t> voted_for() const;

private:
    bool HasMajority(size_t vote_count) const;
    bool IsLogUpToDate(uint64_t candidate_last_log_index, uint64_t candidate_last_log_term) const;
    void BecomeFollower(uint64_t new_term, std::optional<uint32_t> known_leader, uint64_t now_ms);
    void BecomeLeader(uint64_t now_ms);
    void ResetElectionDeadline(uint64_t now_ms);
    uint64_t RandomizedElectionTimeout();

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
