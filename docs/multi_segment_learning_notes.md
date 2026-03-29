# KVDB 多 Segment 实现学习笔记

> 目标：把单日志文件模型（`data_1.log`）演进为可扩展的多 segment 模型（`data_<id>.log`），并保持现有 `Put/Get/Delete + Recover` 语义稳定。

## 1. 问题背景与演进动机

单文件 append-only 虽然简单，但会遇到两个实际问题：

1. **文件无限增长**：长期运行后单日志会非常大，恢复扫描成本线性上升。
2. **运维弹性差**：后续做 merge/compaction 时，单文件模型的切换与回收窗口更难控制。

因此先引入多 segment + rotation，是典型的“先拆分，再治理”路径：

- 写路径仍保持 append-only（不引入随机写复杂度）
- 读路径通过内存索引直接定位 `(file_id, offset)`
- 恢复路径按 `file_id` 顺序扫描，语义与单文件一致

---

## 2. 数据模型变化

### 2.1 文件命名规范

- 旧：固定 `data_1.log`
- 新：`data_<id>.log`（`id` 从 1 递增）

### 2.2 索引模型

`IndexEntry` 原本就包含 `file_id`，多 segment 后该字段真正参与读取路径：

- `Get(key)`：先查内存索引得到 `file_id + offset`
- 再从 `data_files_[file_id]` 读取真实记录

这让“逻辑最新值”与“物理所在文件”解耦。

---

## 3. 打开数据库（Open）设计

`Open()` 关键步骤：

1. 创建数据库目录（若不存在）
2. 扫描目录文件，筛选匹配 `data_<id>.log` 的 segment
3. 对 `id` 去重并升序排序
4. 取最大 `id` 作为 active segment
5. 历史段只读打开，active 段可写打开
6. 调 `Recover()` 顺序回放日志重建内存索引

### 设计要点

- **命名解析稳健性**：仅接受纯数字 `id`，拒绝 `data_x.log` / `data_01a.log`。
- **空库兼容**：目录下没有 segment 时自动从 `data_1.log` 启动。
- **读写职责分离**：历史段负责读，active 段负责写。

---

## 4. 写路径与 Rotation

### 4.1 触发时机

在 `AppendRecord()` 里先估算本条记录大小：

`header + key.size + value.size`

若满足：

`active_size + incoming_record_size > max_data_file_size`

则先执行 `RotateActiveFile()` 再写入。

### 4.2 Rotate 行为

1. 先对当前 active 段 `Sync()`（降低切换窗口内的数据风险）
2. `active_file_id_++`
3. 创建并打开新的 `data_<id>.log` 作为 active
4. 更新 `data_files_` 与 `ordered_file_ids_`

### 4.3 为什么“写前判定”

- 如果“写后判定”，会出现“已超阈值但仍写入旧文件”的状态，边界不清晰。
- 写前判定能保证“进入新段后第一条记录一定写到新段”。

---

## 5. 读取路径（Get）

读取流程不再假设“数据都在 active 文件”：

1. `index_.find(key)` 获取最新位置
2. 用 `entry.file_id` 找到对应 `DataFile`
3. 用 `entry.offset` 读取并校验记录
4. 返回 value（遇 tombstone 返回 NotFound）

### 关键收益

- 历史 segment 上的热点 key 也能被直接命中
- 为后续 merge（重写到新文件、旧段回收）打下索引寻址基础

---

## 6. 恢复路径（Recover）

恢复逻辑升级为“多段顺序回放”：

- 按 `ordered_file_ids_` 升序扫描每个 segment
- `kPut`：更新索引到最新位置
- `kDelete`：从索引移除 key

### 坏尾恢复策略

- 仅对 **active segment** 允许 `IOError/OutOfRange` 时进行截断恢复（`Truncate(offset)`）
- 历史 segment 出现该类错误直接报错

原因：历史段理论上应是稳定封存数据，坏尾更可能代表真实损坏而非崩溃中断。

---

## 7. 工程实践细节

### 7.1 文件名解析采用 `from_chars`

相比 `stoul + try/catch`，`from_chars` 有两个优点：

- 零异常路径，更轻量
- 可显式检查“是否完整消费字符串”

### 7.2 函数注释策略

本次在 `kv_store.h/.cpp` 中对每个核心函数补了注释，建议遵循：

- **函数声明处**：写职责与输入输出语义
- **函数定义处**：写关键步骤与边界策略
- 避免重复“代码显而易见”内容

---

## 8. 测试策略

新增 `TestMultiSegmentRotationAndRecovery` 覆盖：

1. 将 `max_data_file_size` 调小触发 rotation
2. 验证 `data_1.log`、`data_2.log` 均存在
3. 重启后跨 segment 读取多个 key，确认恢复正确

同时保留旧用例，确保：

- 覆盖写
- 删除语义
- CRC/magic/record-type 校验
- 尾部半记录恢复

---

## 9. 当前边界与后续方向

### 当前边界

- 仅支持 rotation，不支持 merge/compaction
- 历史段不会自动回收
- 暂无并发控制（非线程安全）

### 推荐下一步

1. 实现 merge：把有效 key 重写到新段
2. 生成 hint file：降低启动恢复扫描成本
3. 引入 manifest：记录段集合与状态，减少目录扫描歧义
4. 增加并发模型（至少 RWLock + 单写线程）

---

## 10. 一句话总结

多 segment 改造的核心是：

**写入“只追加”不变，索引升级为 `(file_id, offset)`，恢复从“单文件回放”升级为“按段顺序回放”。**

这一步不追求功能完备，而是为后续 merge、回收和性能优化建立清晰的结构化边界。
