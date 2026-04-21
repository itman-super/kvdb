#include <iostream>
#include <filesystem>
#include <string>

#include "raft_log_replication.h"

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

static std::string MakeStatePath(const std::string& name) {
    std::filesystem::path dir = std::filesystem::temp_directory_path() / "kvdb_raft_tests";
    std::filesystem::create_directories(dir);
    return (dir / name).string();
}

void TestCompareLogByTermThenIndex() {
    RaftLogReplication log(1);
    log.AppendLocalEntry(2, "set a=1");
    log.AppendLocalEntry(2, "set b=2");

    ASSERT_TRUE(log.IsLogUpToDate(3, 2));
    ASSERT_TRUE(!log.IsLogUpToDate(1, 2));
    ASSERT_TRUE(log.IsLogUpToDate(1, 3));
    ASSERT_TRUE(!log.IsLogUpToDate(9, 1));
    PassTest(__FUNCTION__);
}

void TestFollowerRejectsMismatchedPrevLog() {
    RaftLogReplication follower(2);
    follower.AppendLocalEntry(1, "x");
    follower.AppendLocalEntry(2, "y");

    RaftLogReplication::AppendEntriesRequest req;
    req.term = 3;
    req.leader_id = 1;
    req.prev_log_index = 2;
    req.prev_log_term = 1;

    auto resp = follower.HandleAppendEntries(req);
    ASSERT_TRUE(!resp.success);
    ASSERT_EQ(resp.conflict_index, 2u);
    PassTest(__FUNCTION__);
}

void TestFollowerReplacesConflictingEntries() {
    RaftLogReplication follower(2);
    follower.AppendLocalEntry(1, "cmd1");
    follower.AppendLocalEntry(1, "cmd2-old");

    RaftLogReplication::AppendEntriesRequest req;
    req.term = 2;
    req.leader_id = 1;
    req.prev_log_index = 1;
    req.prev_log_term = 1;
    req.entries.push_back({2, 2, "cmd2-new"});
    req.entries.push_back({3, 2, "cmd3"});
    req.leader_commit = 2;

    auto resp = follower.HandleAppendEntries(req);
    ASSERT_TRUE(resp.success);
    ASSERT_EQ(resp.match_index, 3u);
    ASSERT_EQ(follower.last_log_index(), 3u);
    ASSERT_EQ(follower.last_log_term(), 2u);
    ASSERT_EQ(follower.commit_index(), 2u);
    ASSERT_TRUE(follower.GetEntry(2).has_value());
    ASSERT_EQ(follower.GetEntry(2)->command, std::string("cmd2-new"));
    PassTest(__FUNCTION__);
}

void TestLeaderAdvancesCommitByMajority() {
    RaftLogReplication leader(1);
    leader.AppendLocalEntry(3, "a");
    leader.AppendLocalEntry(3, "b");
    leader.AppendLocalEntry(3, "c");
    leader.InitLeaderReplication({1, 2, 3});

    RaftLogReplication::AppendEntriesResponse ok;
    ok.term = 3;
    ok.success = true;
    ok.match_index = 3;

    leader.HandleAppendEntriesResponse(2, ok, 3);
    ASSERT_EQ(leader.commit_index(), 3u);

    auto committed = leader.GetCommittedEntriesSince(1);
    ASSERT_EQ(committed.size(), 2u);
    ASSERT_EQ(committed.front().index, 2u);
    ASSERT_EQ(committed.back().index, 3u);
    PassTest(__FUNCTION__);
}

void TestReplicationStatePersistence() {
    const std::string state_path = MakeStatePath("raft_log_replication_state.txt");
    std::filesystem::remove(state_path);

    {
        RaftLogReplication node(1, state_path);
        node.AppendLocalEntry(5, "set x=1");
        node.AppendLocalEntry(5, "set y=2");
        node.InitLeaderReplication({1, 2, 3});

        RaftLogReplication::AppendEntriesResponse ok;
        ok.term = 5;
        ok.success = true;
        ok.match_index = 2;
        node.HandleAppendEntriesResponse(2, ok, 5);
        ASSERT_EQ(node.commit_index(), 2u);
    }

    RaftLogReplication recovered(1, state_path);
    ASSERT_EQ(recovered.last_log_index(), 2u);
    ASSERT_EQ(recovered.last_log_term(), 5u);
    ASSERT_EQ(recovered.commit_index(), 2u);
    ASSERT_TRUE(recovered.GetEntry(2).has_value());
    ASSERT_EQ(recovered.GetEntry(2)->command, std::string("set y=2"));
    PassTest(__FUNCTION__);
}

void TestCreateSnapshotCompactsLog() {
    RaftLogReplication node(1);
    node.AppendLocalEntry(7, "set a=1");
    node.AppendLocalEntry(7, "set b=2");
    node.AppendLocalEntry(7, "set c=3");

    RaftLogReplication::AppendEntriesRequest req;
    req.term = 7;
    req.leader_id = 1;
    req.prev_log_index = 3;
    req.prev_log_term = 7;
    req.leader_commit = 3;
    node.HandleAppendEntries(req);

    ASSERT_TRUE(node.CreateSnapshot(2, "snapshot@2"));
    ASSERT_EQ(node.last_log_index(), 3u);
    ASSERT_TRUE(!node.GetEntry(2).has_value());
    ASSERT_TRUE(node.GetEntry(3).has_value());
    ASSERT_EQ(node.GetEntry(3)->command, std::string("set c=3"));

    auto snap = node.snapshot();
    ASSERT_TRUE(snap.has_value());
    ASSERT_EQ(snap->last_included_index, 2u);
    ASSERT_EQ(snap->last_included_term, 7u);
    ASSERT_EQ(snap->data, std::string("snapshot@2"));
    PassTest(__FUNCTION__);
}

void TestSnapshotPersistenceAndNeedsSnapshot() {
    const std::string state_path = MakeStatePath("raft_log_replication_snapshot_state.txt");
    std::filesystem::remove(state_path);

    {
        RaftLogReplication node(1, state_path);
        node.AppendLocalEntry(9, "k1");
        node.AppendLocalEntry(9, "k2");

        RaftLogReplication::AppendEntriesRequest req;
        req.term = 9;
        req.leader_id = 1;
        req.prev_log_index = 2;
        req.prev_log_term = 9;
        req.leader_commit = 2;
        node.HandleAppendEntries(req);

        ASSERT_TRUE(node.CreateSnapshot(2, "snapshot@term9"));
        node.InitLeaderReplication({1, 2});

        RaftLogReplication::AppendEntriesResponse reject;
        reject.term = 9;
        reject.success = false;
        reject.conflict_index = 1;
        node.HandleAppendEntriesResponse(2, reject, 9);

        ASSERT_TRUE(node.NeedsSnapshot(2));
        auto to_send = node.BuildSnapshotForPeer(2);
        ASSERT_TRUE(to_send.has_value());
        ASSERT_EQ(to_send->last_included_index, 2u);
    }

    RaftLogReplication recovered(1, state_path);
    auto snap = recovered.snapshot();
    ASSERT_TRUE(snap.has_value());
    ASSERT_EQ(snap->last_included_index, 2u);
    ASSERT_EQ(snap->last_included_term, 9u);
    ASSERT_EQ(snap->data, std::string("snapshot@term9"));
    PassTest(__FUNCTION__);
}

int main() {
    TestCompareLogByTermThenIndex();
    TestFollowerRejectsMismatchedPrevLog();
    TestFollowerReplacesConflictingEntries();
    TestLeaderAdvancesCommitByMajority();
    TestReplicationStatePersistence();
    TestCreateSnapshotCompactsLog();
    TestSnapshotPersistenceAndNeedsSnapshot();

    std::cout << "\n==== raft_log_replication_test summary ====\n";
    std::cout << "PASSED: " << g_passed << "\n";
    std::cout << "FAILED: " << g_failed << "\n";

    return g_failed == 0 ? 0 : 1;
}
