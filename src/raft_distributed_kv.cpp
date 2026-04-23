#include "raft_distributed_kv.h"

#include <algorithm>
#include <stdexcept>

namespace {
constexpr char kDelim = '\t';
}

RaftDistributedKV::RaftDistributedKV(std::vector<uint32_t> node_ids) {
    if (node_ids.empty()) {
        throw std::invalid_argument("node_ids must not be empty");
    }
    std::sort(node_ids.begin(), node_ids.end());
    node_ids.erase(std::unique(node_ids.begin(), node_ids.end()), node_ids.end());

    const auto cluster_size = static_cast<uint32_t>(node_ids.size());
    nodes_.reserve(node_ids.size());
    for (uint32_t id : node_ids) {
        if (id == 0) {
            throw std::invalid_argument("node id must be > 0");
        }
        nodes_.emplace_back(id, cluster_size);
        nodes_.back().election.Start(now_ms_);
    }
}

Status RaftDistributedKV::ElectLeader(uint32_t candidate_id) {
    Node* candidate = FindNode(candidate_id);
    if (candidate == nullptr) {
        return Status::NotFound("candidate node not found");
    }

    now_ms_ += 1000;
    LeaderElection::TickAction action = candidate->election.Tick(now_ms_);
    if (action == LeaderElection::TickAction::kNone) {
        return Status::IOError("candidate did not start election");
    }

    if (action == LeaderElection::TickAction::kStartElection) {
        const auto vote_req = candidate->election.BuildRequestVoteRequest();
        for (auto& peer : nodes_) {
            if (peer.id == candidate_id) {
                continue;
            }
            auto vote_resp = peer.election.HandleRequestVote(vote_req, now_ms_);
            candidate->election.HandleRequestVoteResponse(peer.id, vote_resp, now_ms_);
        }
    }

    if (candidate->election.state() != LeaderElection::NodeState::kLeader) {
        return Status::IOError("election failed to produce a leader");
    }

    leader_id_ = candidate_id;
    std::vector<uint32_t> peers;
    peers.reserve(nodes_.size());
    for (const auto& n : nodes_) {
        peers.push_back(n.id);
    }
    candidate->replication.InitLeaderReplication(peers);
    BroadcastHeartbeat(candidate);
    return Status::OK();
}

Status RaftDistributedKV::Put(const std::string& key, const std::string& value) {
    if (key.empty()) {
        return Status::InvalidArgument("key is empty");
    }
    return ApplyWriteCommand(EncodePut(key, value));
}

Status RaftDistributedKV::Delete(const std::string& key) {
    if (key.empty()) {
        return Status::InvalidArgument("key is empty");
    }
    return ApplyWriteCommand(EncodeDelete(key));
}

Status RaftDistributedKV::GetFromNode(uint32_t node_id, const std::string& key, std::string* value) const {
    if (value == nullptr) {
        return Status::InvalidArgument("value is null");
    }
    const Node* node = FindNode(node_id);
    if (node == nullptr) {
        return Status::NotFound("node not found");
    }
    auto it = node->state_machine.find(key);
    if (it == node->state_machine.end()) {
        return Status::NotFound("key not found");
    }
    *value = it->second;
    return Status::OK();
}

std::optional<uint32_t> RaftDistributedKV::leader_id() const {
    return leader_id_;
}

Status RaftDistributedKV::ApplyWriteCommand(const std::string& encoded_cmd) {
    Node* leader = CurrentLeader();
    if (leader == nullptr) {
        return Status::IOError("no leader elected");
    }

    const uint64_t term = leader->election.current_term();
    leader->replication.AppendLocalEntry(term, encoded_cmd);

    for (auto& follower : nodes_) {
        if (follower.id == leader->id) {
            continue;
        }
        Status s = ReplicateToFollower(leader, &follower);
        if (!s.ok()) {
            return s;
        }
    }

    BroadcastHeartbeat(leader);
    for (auto& node : nodes_) {
        ApplyCommittedEntries(&node);
    }
    return Status::OK();
}

void RaftDistributedKV::ApplyCommittedEntries(Node* node) {
    const auto entries = node->replication.GetCommittedEntriesSince(node->last_applied);
    for (const auto& entry : entries) {
        bool is_delete = false;
        std::string key;
        std::string value;
        if (!DecodeCommand(entry.command, &is_delete, &key, &value)) {
            continue;
        }
        if (is_delete) {
            node->state_machine.erase(key);
        } else {
            node->state_machine[key] = value;
        }
        node->last_applied = entry.index;
    }
}

Status RaftDistributedKV::ReplicateToFollower(Node* leader, Node* follower) {
    if (leader == nullptr || follower == nullptr) {
        return Status::InvalidArgument("leader/follower is null");
    }

    const uint64_t term = leader->election.current_term();
    for (int i = 0; i < 64; ++i) {
        auto req = leader->replication.BuildAppendEntriesRequest(follower->id, term, leader->id, 16);
        auto resp = follower->replication.HandleAppendEntries(req);
        leader->replication.HandleAppendEntriesResponse(follower->id, resp, term);

        if (!resp.success) {
            if (leader->replication.NeedsSnapshot(follower->id)) {
                auto snap = leader->replication.BuildSnapshotForPeer(follower->id);
                if (snap.has_value()) {
                    follower->replication.InstallSnapshot(*snap);
                }
            }
            continue;
        }

        if (resp.match_index >= leader->replication.last_log_index()) {
            return Status::OK();
        }
    }
    return Status::IOError("replication retries exhausted");
}

void RaftDistributedKV::BroadcastHeartbeat(Node* leader) {
    if (leader == nullptr) {
        return;
    }
    const uint64_t term = leader->election.current_term();
    for (auto& follower : nodes_) {
        if (follower.id == leader->id) {
            continue;
        }
        auto req = leader->replication.BuildAppendEntriesRequest(follower.id, term, leader->id, 0);
        req.entries.clear();
        auto resp = follower.replication.HandleAppendEntries(req);
        leader->replication.HandleAppendEntriesResponse(follower.id, resp, term);
    }
}

std::string RaftDistributedKV::EncodePut(const std::string& key, const std::string& value) {
    return std::string("P") + kDelim + key + kDelim + value;
}

std::string RaftDistributedKV::EncodeDelete(const std::string& key) {
    return std::string("D") + kDelim + key;
}

bool RaftDistributedKV::DecodeCommand(const std::string& encoded,
                                      bool* is_delete,
                                      std::string* key,
                                      std::string* value) {
    if (is_delete == nullptr || key == nullptr || value == nullptr || encoded.size() < 3) {
        return false;
    }

    const size_t first = encoded.find(kDelim);
    if (first == std::string::npos || first == 0) {
        return false;
    }

    const char op = encoded[0];
    if (op == 'P') {
        const size_t second = encoded.find(kDelim, first + 1);
        if (second == std::string::npos) {
            return false;
        }
        *is_delete = false;
        *key = encoded.substr(first + 1, second - first - 1);
        *value = encoded.substr(second + 1);
        return !key->empty();
    }

    if (op == 'D') {
        *is_delete = true;
        *key = encoded.substr(first + 1);
        value->clear();
        return !key->empty();
    }

    return false;
}

RaftDistributedKV::Node* RaftDistributedKV::FindNode(uint32_t node_id) {
    auto it = std::find_if(nodes_.begin(), nodes_.end(), [node_id](const Node& node) {
        return node.id == node_id;
    });
    return it == nodes_.end() ? nullptr : &(*it);
}

const RaftDistributedKV::Node* RaftDistributedKV::FindNode(uint32_t node_id) const {
    auto it = std::find_if(nodes_.begin(), nodes_.end(), [node_id](const Node& node) {
        return node.id == node_id;
    });
    return it == nodes_.end() ? nullptr : &(*it);
}

RaftDistributedKV::Node* RaftDistributedKV::CurrentLeader() {
    if (!leader_id_.has_value()) {
        return nullptr;
    }
    Node* leader = FindNode(*leader_id_);
    if (leader == nullptr || leader->election.state() != LeaderElection::NodeState::kLeader) {
        return nullptr;
    }
    return leader;
}
