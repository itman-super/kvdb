# Raft 状态持久化学习笔记

## 1. 目标

这次改造的目标是：**把会影响 Raft 逻辑分支判断的运行时状态外存化**，确保进程重启后状态机行为连续，不因为“内存清零”而走出错误路径。

---

## 2. 为什么要做“状态外存”

如果仅靠内存保存状态，进程重启后会丢失：

- 选举任期、投票对象；
- 已有日志条目和提交位点；
- Leader 对各 Follower 的复制游标（`nextIndex` / `matchIndex`）。

这些状态一旦丢失，可能导致：

- 同任期重复投票；
- 日志回退和冲突处理行为不稳定；
- 已提交日志在重启后看起来“未提交”。

因此需要在状态变更点同步写盘。

---

## 3. 本次外存化覆盖范围

### 3.1 LeaderElection

持久化字段（`state_file_path`）：

- `state_`（Follower/Candidate/Leader）；
- `current_term_`；
- `voted_for_`；
- `leader_id_`；
- `last_log_index_`；
- `last_log_term_`。

触发写盘时机：

- 进入 Candidate、Follower、Leader；
- 投票授予成功后；
- 处理投票响应后票数更新；
- 更新本地最后日志信息后。

### 3.2 RaftLogReplication

持久化字段（构造参数 `state_file_path`）：

- `log_`；
- `commit_index_`；
- `next_index_`；
- `match_index_`。

触发写盘时机：

- `Reset()`；
- 本地追加日志；
- 初始化 Leader 复制进度；
- 处理 `AppendEntriesResponse`；
- Follower 应用 `AppendEntries` 成功后。

---

## 4. 文件格式设计（当前实现）

为了便于调试，采用**纯文本格式**：

- LeaderElection：单行顺序字段；
- RaftLogReplication：头部 + log 段 + next/match map 段。

优点：

- 可读性好，手工排查方便；
- 实现简单，学习成本低。

风险与后续优化：

- 命令字符串若包含换行，需要转义或改二进制编码；
- 建议后续增加 magic/version + checksum；
- 可采用“写临时文件 + rename”做原子落盘，避免半写状态。

---

## 5. 设计取舍

1. **兼容性**：默认路径为空即关闭持久化，保持旧测试与旧调用方式可用。  
2. **最小侵入**：不引入额外组件，状态在类内部自管理。  
3. **先正确后性能**：当前每次关键状态变化都触发写盘，优先保证语义连续。后续可做批量 flush 或 WAL 化。  

---

## 6. 如何验证

新增了重启恢复类测试：

- `TestElectionStatePersistence`
- `TestReplicationStatePersistence`

两者都采用“先写状态 -> 析构对象 -> 重新构造并读取 -> 校验关键字段”的方式，验证持久化链路完整。

---

## 7. 后续可演进方向

1. 统一抽象 `PersistentStateStore`，避免选举/复制模块重复序列化代码；  
2. 引入校验和与版本升级策略；  
3. 用 fsync 策略与批处理降低频繁写盘开销；  
4. 把 KVStore 的现有 snapshot/hint 思路与 Raft 状态持久化策略统一成同一套元数据规范。  
