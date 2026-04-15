#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

class RaftLogReplication {
public:
    struct LogEntry {
        uint64_t index = 0;
        uint64_t term = 0;
        std::string command;
    };

    struct AppendEntriesRequest {
        uint64_t term = 0;
        uint32_t leader_id = 0;
        uint64_t prev_log_index = 0;
        uint64_t prev_log_term = 0;
        std::vector<LogEntry> entries;
        uint64_t leader_commit = 0;
    };

    struct AppendEntriesResponse {
        uint64_t term = 0;
        bool success = false;
        uint64_t match_index = 0;
        uint64_t conflict_index = 0;
    };

    explicit RaftLogReplication(uint32_t node_id);

    void Reset();

    uint64_t last_log_index() const;
    uint64_t last_log_term() const;
    uint64_t commit_index() const;

    bool IsLogUpToDate(uint64_t candidate_last_log_index, uint64_t candidate_last_log_term) const;

    uint64_t AppendLocalEntry(uint64_t current_term, std::string command);

    void InitLeaderReplication(const std::vector<uint32_t>& peer_ids);
    AppendEntriesRequest BuildAppendEntriesRequest(uint32_t peer_id,
                                                   uint64_t current_term,
                                                   uint32_t leader_id,
                                                   size_t max_entries = 32) const;
    void HandleAppendEntriesResponse(uint32_t peer_id,
                                     const AppendEntriesResponse& response,
                                     uint64_t current_term);

    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request);

    std::optional<LogEntry> GetEntry(uint64_t log_index) const;
    std::vector<LogEntry> GetCommittedEntriesSince(uint64_t last_applied) const;

private:
    size_t ToVectorPos(uint64_t log_index) const;
    uint64_t FindConflictIndex(uint64_t prev_log_index) const;
    uint64_t FindTerm(uint64_t log_index) const;

private:
    uint32_t node_id_;
    std::vector<LogEntry> log_;
    uint64_t commit_index_ = 0;

    std::unordered_map<uint32_t, uint64_t> next_index_;
    std::unordered_map<uint32_t, uint64_t> match_index_;
};
