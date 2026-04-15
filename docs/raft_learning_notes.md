# Raft 学习笔记：日志同步、领导选举与日志比较

## 1. 目标拆解

本次改造聚焦三件事：

1. **增加日志同步模块**：独立维护 Raft 日志复制状态（`nextIndex`/`matchIndex`/`commitIndex`）。
2. **完善领导选举**：补上边界处理（单节点集群快速成为 Leader、非法候选人过滤）。
3. **日志比较规则明确化**：候选人日志是否“更新”的判断遵循 `lastLogTerm` 优先、`lastLogIndex` 次级。

---

## 2. 日志同步模块设计（`RaftLogReplication`）

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

## 3. 领导选举改进点（`LeaderElection`）

### 3.1 单节点集群快速当选

之前单节点超时后虽然会触发选举，但对外动作仍是 `kStartElection`。现在在 `Tick` 中若自票即达多数，立即 `BecomeLeader()` 并返回 `kSendHeartbeat`，让上层可以马上发送心跳/空复制请求，行为更符合 Raft 实践实现。

### 3.2 非法候选人防御

在 `HandleRequestVote` 增加 `candidate_id == 0` 拒绝逻辑，避免被非法输入污染投票状态。

### 3.3 多数判定抽象

新增 `HasMajority(vote_count)`，统一选举阶段和投票响应阶段的多数判定，降低重复逻辑与未来改错成本。

---

## 4. 日志比较（Log Up-to-date）规则总结

无论在选举模块还是日志复制模块，都遵循一致判定：

1. `candidate_last_log_term > local_last_log_term`：候选者更新。
2. 若 term 相同，则比较 index：`candidate_last_log_index >= local_last_log_index` 才算更新。

这个规则确保“较新任期日志优先”，避免旧任期但更长的日志错误当选。

---

## 5. 测试策略

新增与增强测试覆盖以下场景：

- 日志比较：term 优先、index 次级。
- Follower 对 `prev_log` 不匹配请求进行拒绝并返回冲突位置。
- Follower 发生冲突时截断并替换日志。
- Leader 收到多数复制确认后推进 `commitIndex`。
- 领导选举新增：单节点立即成为 Leader；拒绝 `candidate_id=0` 投票请求。

---

## 6. 后续可继续演进的方向

1. 持久化 `currentTerm` / `votedFor` / Raft 日志（当前为内存实现，适合教学）。
2. 引入状态机应用层（`lastApplied` + apply loop）。
3. 增加快照与 InstallSnapshot RPC。
4. 把选举模块与日志模块整合为统一 `RaftNode`，减少外层协调复杂度。
5. 增加网络故障注入测试（乱序、重复、分区、延迟）验证安全性与活性。

