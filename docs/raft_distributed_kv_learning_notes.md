# 基于 Raft 的分布式 KV：实现学习笔记

## 1. 目标

本次补全的目标是把现有的 `LeaderElection` + `RaftLogReplication` 串起来，形成一个可验证一致性行为的教学版分布式 KV：

- 写请求只能从 Leader 进入；
- 通过 AppendEntries 复制到 Follower；
- 由多数派推进 `commitIndex`；
- 每个节点把已提交日志 apply 到本地状态机（`std::unordered_map`）。

> 这不是生产级实现（没有真实网络、没有 WAL 到状态机的完整持久化闭环），但结构上对齐了 Raft 的核心路径。

---

## 2. 模块拆分

新增 `RaftDistributedKV`（`include/raft_distributed_kv.h` + `src/raft_distributed_kv.cpp`），内部每个节点包含：

1. `LeaderElection election`：负责角色与任期；
2. `RaftLogReplication replication`：负责日志、提交点与复制状态；
3. `state_machine`：已 apply 的键值状态；
4. `last_applied`：跟踪状态机进度。

这种拆分对应 Raft 的常见三层：

- **共识控制层**（选举 + 复制）
- **日志层**（ordered log）
- **状态机层**（deterministic apply）

---

## 3. 关键写路径（Put/Delete）

### Step A：Leader 本地追加

Leader 先调用 `AppendLocalEntry(term, command)`，确保日志顺序号增长。

### Step B：复制到 Follower

按 follower 循环调用：

- `BuildAppendEntriesRequest(peer)` 构造请求；
- follower 执行 `HandleAppendEntries`；
- leader 执行 `HandleAppendEntriesResponse` 更新 `nextIndex/matchIndex`。

若 follower 落后到快照边界前，利用 `NeedsSnapshot` + `BuildSnapshotForPeer` + `InstallSnapshot` 补齐。

### Step C：提交与 apply

复制后再广播一次空 AppendEntries（heartbeat），把 leader 最新 `commitIndex` 带给 follower；随后每个节点执行：

- `GetCommittedEntriesSince(last_applied)`
- 逐条解析命令并更新本地 map
- 前移 `last_applied`

这样就完整形成了 **log replication -> commit -> state machine apply** 的闭环。

---

## 4. 命令编码策略

为了让日志条目可直接回放，本次采用简单文本命令编码：

- Put：`P\t<key>\t<value>`
- Delete：`D\t<key>`

apply 时解码后分别执行 `map[key] = value` 与 `map.erase(key)`。

说明：真实系统通常会用 protobuf/flatbuffers，并且要处理转义、版本与兼容性；本实现优先强调流程清晰。

---

## 5. 选主流程（模拟）

`ElectLeader(candidate_id)` 的流程：

1. 推进候选节点时钟，触发 `Tick` 进入竞选；
2. 候选节点广播 `RequestVote`；
3. 收集响应并处理，若过半则成为 leader；
4. 初始化复制状态 `InitLeaderReplication` 并发送首轮心跳。

这个流程把选举模块与日志复制模块衔接起来，避免“有 leader 选举但不能承接写入”的割裂。

---

## 6. 测试覆盖与验证点

新增 `tests/raft_distributed_kv_test.cpp`，覆盖三类场景：

1. **选主 + Put 一致性**：写入后 3 个节点都读到同值；
2. **Delete 一致性**：删除后 3 个节点都返回 NotFound；
3. **无主写保护**：未选主时写请求返回错误。

这些测试重点验证“行为正确性”而非性能。

---

## 7. 当前边界与后续演进

当前边界：

- 单进程同步“伪网络”；
- 没有 membership 变更；
- 没有线性一致读（read-index / lease read）；
- 状态机快照与底层 KVStore 还未打通。

后续可演进：

1. 引入网络抽象层（超时、乱序、丢包注入）；
2. 将状态机切换为现有 `KVStore`，实现真实磁盘状态机；
3. 增加 `ReadIndex` 流程支持线性一致读；
4. 增加 joint consensus 支持成员变更。
