# Raft 学习笔记：日志同步、领导选举、日志比较与 Snapshot 压缩

## 1. 目标拆解

本次改造聚焦四件事：

1. **增加日志同步模块**：独立维护 Raft 日志复制状态（`nextIndex`/`matchIndex`/`commitIndex`）。
2. **完善领导选举**：补上边界处理（单节点集群快速成为 Leader、非法候选人过滤）。
3. **日志比较规则明确化**：候选人日志是否“更新”的判断遵循 `lastLogTerm` 优先、`lastLogIndex` 次级。
4. **增加 Snapshot 压缩能力**：在日志已提交后截断前缀，减少重放成本，并为落后节点提供快照补齐入口。

---

## 2. Snapshot 压缩实现思路（`RaftLogReplication`）

### 2.1 为什么要做 Snapshot

Raft 日志会持续增长；如果不压缩：

- 重启恢复要扫描更长日志；
- 落后节点追赶需要回放大量历史 entry；
- 磁盘占用与内存索引开销持续上升。

因此引入快照边界：`last_included_index/term`。

### 2.2 核心状态变化

在日志复制状态机中增加：

- `snapshot_.last_included_index`
- `snapshot_.last_included_term`
- `snapshot_.data`

并将“日志起点”从固定 index=1 改为“快照后第一条日志”，即：

- `last_log_index()`：当 `log_` 为空时返回 `snapshot_.last_included_index`
- `last_log_term()`：当 `log_` 为空时返回 `snapshot_.last_included_term`
- `ToVectorPos(log_index)`：使用 `log_index - snapshot_last_included_index - 1` 做偏移换算

### 2.3 CreateSnapshot（本地压缩）

触发条件：

- `last_included_index` 必须前进；
- 且不能超过 `commit_index_`（只压缩已提交日志）。

执行步骤：

1. 读取 `last_included_index` 对应任期作为 `last_included_term`。
2. 更新 `snapshot_` 元数据并保存快照内容。
3. 删除日志前缀（`<= last_included_index`）。
4. 持久化状态。

### 2.4 复制阶段的兼容处理

#### AppendEntries 构建

若某 follower 的 `nextIndex <= snapshot.last_included_index`，说明其已落后到快照之前：

- AppendEntries 不再尝试发送已压缩日志；
- 通过 `NeedsSnapshot(peer)` 标记该 follower 需要快照补齐。

#### Follower 接收 AppendEntries

当请求的 `prev_log_index < snapshot.last_included_index` 时直接拒绝，并返回 `conflict_index = snapshot.last_included_index + 1`，提示 leader 改为发快照。

### 2.5 InstallSnapshot（接收端应用快照）

当前实现提供 `InstallSnapshot(snapshot)`：

1. 若快照不新（index 未前进）直接忽略。
2. 更新本地 `snapshot_`。
3. 删除已被快照覆盖的日志前缀。
4. 至少把 `commit_index_` 推进到 `snapshot.last_included_index`。

这为后续扩展 `InstallSnapshot RPC` 提供了状态机基础。

### 2.6 持久化格式调整

原持久化仅保存 `(commit + log + nextIndex + matchIndex)`，新增首行快照元信息：

`snapshot_last_index snapshot_last_term snapshot_data`

重启后可恢复快照边界与快照内容，避免“重启后又从 1 开始”的错误行为。

### 2.7 测试覆盖

新增测试点：

- `CreateSnapshot` 后日志前缀被裁剪；
- `snapshot()` 能返回正确 index/term/data；
- 快照状态可持久化并在重启后恢复；
- follower 回退过深时可被判定为 `NeedsSnapshot`。

---

## 3. 日志同步模块设计（`RaftLogReplication`）

### 2.1 状态建模

- `log_`: 顺序日志，记录 `(index, term, command)`。
- `commit_index_`: 已提交最大索引。
- `next_index_[peer]`: Leader 预计发送给每个 Follower 的下一条日志索引。
- `match_index_[peer]`: Leader 已确认某个 Follower 匹配到的最大索引。

这四个状态是 Raft 论文中日志复制最关键的数据结构。

### 2.2 Leader 发送 AppendEntries 的构造

`BuildAppendEntriesRequest(peer)` 的流程：

1. 用 `next_index_[peer]` 决定从哪条日志开始发。
2. 自动计算 `prev_log_index = next - 1` 与 `prev_log_term`。
3. 按 `max_entries` 做分批发送（便于后续流控与限流）。

### 2.3 Follower 处理 AppendEntries

`HandleAppendEntries(request)` 关键步骤：

1. 若 `prev_log_index` 超出本地日志末尾，拒绝并返回 `conflict_index = last_log_index + 1`。
2. 若 `prev_log_term` 不匹配，拒绝并返回冲突 term 的起始索引（加速 Leader 回退）。
3. 匹配后，遇到冲突条目就截断本地尾部，再追加 Leader 新条目。
4. 用 `min(leader_commit, last_log_index)` 推进本地 `commit_index`。

### 2.4 Leader 处理响应与提交推进

`HandleAppendEntriesResponse(peer, response)`：

- 成功：推进该 follower 的 `matchIndex/nextIndex`。
- 失败：按 `conflict_index` 回退 `nextIndex`（若无冲突索引则线性回退 1）。
- 提交判定：把所有节点匹配索引（含 Leader 自己）排序，取中位数（多数派位置）作为可提交候选；且只提交**当前任期**日志。

---

## 4. 领导选举改进点（`LeaderElection`）

### 3.1 单节点集群快速当选

之前单节点超时后虽然会触发选举，但对外动作仍是 `kStartElection`。现在在 `Tick` 中若自票即达多数，立即 `BecomeLeader()` 并返回 `kSendHeartbeat`，让上层可以马上发送心跳/空复制请求，行为更符合 Raft 实践实现。

### 3.2 非法候选人防御

在 `HandleRequestVote` 增加 `candidate_id == 0` 拒绝逻辑，避免被非法输入污染投票状态。

### 3.3 多数判定抽象

新增 `HasMajority(vote_count)`，统一选举阶段和投票响应阶段的多数判定，降低重复逻辑与未来改错成本。

---

## 5. 日志比较（Log Up-to-date）规则总结

无论在选举模块还是日志复制模块，都遵循一致判定：

1. `candidate_last_log_term > local_last_log_term`：候选者更新。
2. 若 term 相同，则比较 index：`candidate_last_log_index >= local_last_log_index` 才算更新。

这个规则确保“较新任期日志优先”，避免旧任期但更长的日志错误当选。

---

## 6. 测试策略

新增与增强测试覆盖以下场景：

- 日志比较：term 优先、index 次级。
- Follower 对 `prev_log` 不匹配请求进行拒绝并返回冲突位置。
- Follower 发生冲突时截断并替换日志。
- Leader 收到多数复制确认后推进 `commitIndex`。
- 领导选举新增：单节点立即成为 Leader；拒绝 `candidate_id=0` 投票请求。

---

## 7. 后续可继续演进的方向

1. 持久化 `currentTerm` / `votedFor` / Raft 日志（当前为内存实现，适合教学）。
2. 引入状态机应用层（`lastApplied` + apply loop）。
3. 增加快照与 InstallSnapshot RPC。
4. 把选举模块与日志模块整合为统一 `RaftNode`，减少外层协调复杂度。
5. 增加网络故障注入测试（乱序、重复、分区、延迟）验证安全性与活性。

## 本轮增强：网络抽象、真实状态机、ReadIndex、Joint Consensus

- **网络抽象层（教学版）**：在 `RaftDistributedKV::NetworkConfig` 中新增三类注入能力：
  - `timeout_inject_mod`：按 RPC 序号周期性模拟超时；
  - `drop_inject_mod`：按 RPC 序号周期性模拟丢包；
  - `reorder_responses`：对响应做延迟队列重排，模拟乱序可见性。
- **状态机切换为真实 KVStore**：每个节点从原先内存 `unordered_map` 改为独立 `KVStore` 实例，数据落到各自目录（`/tmp/kvdb_raft_node_<id>`），Apply 阶段通过 `Put/Delete` 写盘。
- **ReadIndex 线性一致读**：新增 `ReadIndexGet`，先经由 `EnsureReadBarrier` 与多数派完成一次读屏障，再读取节点状态机，近似体现“读前确认当前 leader 仍具法定多数”。
- **Joint Consensus 成员变更（简化实现）**：新增 `ChangeMembershipJoint`：
  1. 进入 `old,new` 联合配置；
  2. 统计复制成功节点集合；
  3. 同时满足 old/new 两侧多数派才提交；
  4. 收敛到新配置。

> 说明：以上是偏教学的最小闭环实现，用于帮助理解机制；未覆盖生产级网络时钟、重试预算、持久化配置项、快照安装驱动下的成员变更边界条件。
