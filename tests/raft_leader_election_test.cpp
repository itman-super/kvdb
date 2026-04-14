#include <iostream>
#include <string>

#include "raft_leader_election.h"

static int g_passed = 0;
static int g_failed = 0;

#define ASSERT_TRUE(expr)                                                     \
    do {                                                                      \
        if (!(expr)) {                                                        \
            std::cerr << "[FAILED] " << __FUNCTION__                        \
                      << " | ASSERT_TRUE(" #expr ") at line " << __LINE__   \
                      << std::endl;                                           \
            ++g_failed;                                                       \
            return;                                                           \
        }                                                                     \
    } while (0)

#define ASSERT_EQ(lhs, rhs)                                                   \
    do {                                                                      \
        auto _lhs = (lhs);                                                    \
        auto _rhs = (rhs);                                                    \
        if (!(_lhs == _rhs)) {                                                \
            std::cerr << "[FAILED] " << __FUNCTION__                        \
                      << " | ASSERT_EQ(" #lhs ", " #rhs ") at line "       \
                      << __LINE__ << std::endl;                               \
            ++g_failed;                                                       \
            return;                                                           \
        }                                                                     \
    } while (0)

static void PassTest(const std::string& name) {
    std::cout << "[PASSED] " << name << std::endl;
    ++g_passed;
}

static LeaderElection::Config MakeConfig(uint32_t node_id = 1) {
    LeaderElection::Config config;
    config.node_id = node_id;
    config.cluster_size = 3;
    config.min_election_timeout_ms = 10;
    config.max_election_timeout_ms = 10;
    config.heartbeat_interval_ms = 5;
    config.random_seed = 7;
    return config;
}

void TestStartElectionOnTimeout() {
    LeaderElection election(MakeConfig(1));
    election.Start(100);

    auto action = election.Tick(110);

    ASSERT_EQ(action, LeaderElection::TickAction::kStartElection);
    ASSERT_EQ(election.state(), LeaderElection::NodeState::kCandidate);
    ASSERT_EQ(election.current_term(), 1u);
    ASSERT_TRUE(election.voted_for().has_value());
    ASSERT_EQ(election.voted_for().value(), 1u);
    PassTest(__FUNCTION__);
}

void TestBecomeLeaderAfterMajorityVotes() {
    LeaderElection election(MakeConfig(1));
    election.Start(0);
    ASSERT_EQ(election.Tick(10), LeaderElection::TickAction::kStartElection);

    LeaderElection::RequestVoteResponse response;
    response.term = 1;
    response.vote_granted = true;

    const bool became_leader = election.HandleRequestVoteResponse(2, response, 11);

    ASSERT_TRUE(became_leader);
    ASSERT_EQ(election.state(), LeaderElection::NodeState::kLeader);
    ASSERT_TRUE(election.leader_id().has_value());
    ASSERT_EQ(election.leader_id().value(), 1u);
    PassTest(__FUNCTION__);
}

void TestRejectStaleRequestVote() {
    LeaderElection election(MakeConfig(1));
    election.Start(0);
    ASSERT_EQ(election.Tick(10), LeaderElection::TickAction::kStartElection);

    LeaderElection::RequestVoteRequest request;
    request.term = 0;
    request.candidate_id = 2;
    request.last_log_index = 1;
    request.last_log_term = 1;

    auto response = election.HandleRequestVote(request, 12);
    ASSERT_TRUE(!response.vote_granted);
    ASSERT_EQ(response.term, 1u);
    PassTest(__FUNCTION__);
}

void TestGrantVoteWithUpToDateLog() {
    LeaderElection election(MakeConfig(1));
    election.Start(0);
    election.UpdateLastLog(10, 3);

    LeaderElection::RequestVoteRequest request;
    request.term = 2;
    request.candidate_id = 2;
    request.last_log_index = 10;
    request.last_log_term = 3;

    auto response = election.HandleRequestVote(request, 5);
    ASSERT_TRUE(response.vote_granted);
    ASSERT_EQ(response.term, 2u);
    ASSERT_TRUE(election.voted_for().has_value());
    ASSERT_EQ(election.voted_for().value(), 2u);
    PassTest(__FUNCTION__);
}

void TestAppendEntriesDemotesCandidate() {
    LeaderElection election(MakeConfig(1));
    election.Start(0);
    ASSERT_EQ(election.Tick(10), LeaderElection::TickAction::kStartElection);

    LeaderElection::AppendEntriesRequest request;
    request.term = 2;
    request.leader_id = 3;

    auto response = election.HandleAppendEntries(request, 12);
    ASSERT_TRUE(response.success);
    ASSERT_EQ(response.term, 2u);
    ASSERT_EQ(election.state(), LeaderElection::NodeState::kFollower);
    ASSERT_TRUE(election.leader_id().has_value());
    ASSERT_EQ(election.leader_id().value(), 3u);
    PassTest(__FUNCTION__);
}

int main() {
    TestStartElectionOnTimeout();
    TestBecomeLeaderAfterMajorityVotes();
    TestRejectStaleRequestVote();
    TestGrantVoteWithUpToDateLog();
    TestAppendEntriesDemotesCandidate();

    std::cout << "\n==== raft_leader_election_test summary ====\n";
    std::cout << "PASSED: " << g_passed << "\n";
    std::cout << "FAILED: " << g_failed << "\n";

    return g_failed == 0 ? 0 : 1;
}
