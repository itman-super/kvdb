#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv_store.h"
#include "raft_leader_election.h"
#include "raft_log_replication.h"
#include "status.h"

/// @brief 基于 Raft（选举 + 日志复制）的教学型分布式 KV 集群模拟器。
///
/// 说明：
/// - 该类不实现真实网络，仅在进程内同步调用模拟 RPC；
/// - 写请求仅允许由 leader 接收，随后复制到多数节点并提交；
/// - 各节点维护独立状态机（in-memory map），用于验证一致性语义。
class RaftDistributedKV {
public:
    struct NetworkConfig {
        uint32_t timeout_inject_mod = 0;
        uint32_t drop_inject_mod = 0;
        bool reorder_responses = false;
    };

    /// @brief 构造一个固定成员列表的 Raft 集群。
    explicit RaftDistributedKV(std::vector<uint32_t> node_ids);
    RaftDistributedKV(std::vector<uint32_t> node_ids, NetworkConfig network);

    /// @brief 发起一次由 candidate_id 主导的选举；成功后会更新当前 leader。
    Status ElectLeader(uint32_t candidate_id);

    /// @brief 在 leader 上执行 Put，并复制到集群。
    Status Put(const std::string& key, const std::string& value);

    /// @brief 在 leader 上执行 Delete，并复制到集群。
    Status Delete(const std::string& key);

    /// @brief 从指定节点读取 key。
    Status GetFromNode(uint32_t node_id, const std::string& key, std::string* value) const;

    /// @brief 返回当前 leader（若尚未选主则返回 nullopt）。
    std::optional<uint32_t> leader_id() const;

    /// @brief ReadIndex 线性一致读：通过 leader 屏障读取指定节点状态机值。
    Status ReadIndexGet(uint32_t node_id, const std::string& key, std::string* value);

    /// @brief joint consensus 成员变更（简化版教学实现）。
    Status ChangeMembershipJoint(const std::vector<uint32_t>& new_members);

private:
    struct Node {
        uint32_t id = 0;
        LeaderElection election;
        RaftLogReplication replication;
        std::unique_ptr<KVStore> state_machine;
        uint64_t last_applied = 0;
        std::string data_dir;

        explicit Node(uint32_t node_id, uint32_t cluster_size)
            : id(node_id),
              election(LeaderElection::Config{node_id, cluster_size, 150, 300, 50, node_id * 7 + 11, ""}),
              replication(node_id) {}
    };

    Status ApplyWriteCommand(const std::string& encoded_cmd);
    void ApplyCommittedEntries(Node* node);
    Status ReplicateToFollower(Node* leader, Node* follower);
    void BroadcastHeartbeat(Node* leader);

    static std::string EncodePut(const std::string& key, const std::string& value);
    static std::string EncodeDelete(const std::string& key);
    static bool DecodeCommand(const std::string& encoded,
                              bool* is_delete,
                              std::string* key,
                              std::string* value);

    Node* FindNode(uint32_t node_id);
    const Node* FindNode(uint32_t node_id) const;
    Node* CurrentLeader();
    bool MajorityAccepted(const std::unordered_set<uint32_t>& voters,
                          const std::unordered_set<uint32_t>& accepted) const;
    Status EnsureReadBarrier(Node* leader);

private:
    NetworkConfig network_;
    std::vector<Node> nodes_;
    std::unordered_set<uint32_t> members_;
    std::unordered_set<uint32_t> joint_old_members_;
    std::unordered_set<uint32_t> joint_new_members_;
    uint64_t rpc_seq_ = 0;
    std::optional<uint32_t> leader_id_;
    uint64_t now_ms_ = 0;
};
