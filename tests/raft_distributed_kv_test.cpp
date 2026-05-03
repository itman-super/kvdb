#include <iostream>
#include <string>

#include "raft_distributed_kv.h"

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

void TestElectLeaderAndReplicatePut() {
    RaftDistributedKV cluster({1, 2, 3});
    ASSERT_TRUE(cluster.ElectLeader(1).ok());
    ASSERT_TRUE(cluster.leader_id().has_value());
    ASSERT_EQ(cluster.leader_id().value(), 1u);

    ASSERT_TRUE(cluster.Put("name", "raft-kv").ok());

    std::string v1;
    std::string v2;
    std::string v3;
    ASSERT_TRUE(cluster.GetFromNode(1, "name", &v1).ok());
    ASSERT_TRUE(cluster.GetFromNode(2, "name", &v2).ok());
    ASSERT_TRUE(cluster.GetFromNode(3, "name", &v3).ok());
    ASSERT_EQ(v1, std::string("raft-kv"));
    ASSERT_EQ(v2, std::string("raft-kv"));
    ASSERT_EQ(v3, std::string("raft-kv"));
    PassTest(__FUNCTION__);
}

void TestDeleteReplicatedToFollowers() {
    RaftDistributedKV cluster({1, 2, 3});
    ASSERT_TRUE(cluster.ElectLeader(2).ok());
    ASSERT_TRUE(cluster.Put("k", "v").ok());
    ASSERT_TRUE(cluster.Delete("k").ok());

    std::string value;
    ASSERT_EQ(cluster.GetFromNode(1, "k", &value).code(), Status::kNotFound);
    ASSERT_EQ(cluster.GetFromNode(2, "k", &value).code(), Status::kNotFound);
    ASSERT_EQ(cluster.GetFromNode(3, "k", &value).code(), Status::kNotFound);
    PassTest(__FUNCTION__);
}

void TestRejectWriteWithoutLeader() {
    RaftDistributedKV cluster({1, 2, 3});
    ASSERT_EQ(cluster.Put("x", "1").code(), Status::kIOError);
    PassTest(__FUNCTION__);
}


void TestReadIndexAndJointConsensus() {
    RaftDistributedKV::NetworkConfig net;
    net.reorder_responses = true;
    RaftDistributedKV cluster({1, 2, 3}, net);
    ASSERT_TRUE(cluster.ElectLeader(1).ok());
    ASSERT_TRUE(cluster.Put("rk", "v1").ok());
    std::string value;
    ASSERT_TRUE(cluster.ReadIndexGet(2, "rk", &value).ok());
    ASSERT_EQ(value, std::string("v1"));
    ASSERT_TRUE(cluster.ChangeMembershipJoint({1, 2, 3}).ok());
    PassTest(__FUNCTION__);
}

int main() {
    TestElectLeaderAndReplicatePut();
    TestDeleteReplicatedToFollowers();
    TestRejectWriteWithoutLeader();
    TestReadIndexAndJointConsensus();

    std::cout << "\n==== raft_distributed_kv_test summary ====\n";
    std::cout << "PASSED: " << g_passed << "\n";
    std::cout << "FAILED: " << g_failed << "\n";

    return g_failed == 0 ? 0 : 1;
}
