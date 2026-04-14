# kvdb

一个使用 **C++17** 实现的轻量级、Bitcask 风格键值存储原型。

项目采用 **append-only 日志 + 内存哈希索引** 设计：写入只追加到日志末尾，读取通过内存索引定位偏移，启动时扫描日志（或加载快照/hint）恢复最新状态。适合学习存储引擎内核原理与实验，不以生产可用性为目标。

---

## 目录

- [特性概览](#特性概览)
- [架构设计](#架构设计)
- [项目结构](#项目结构)
- [核心模块说明](#核心模块说明)
- [磁盘文件格式](#磁盘文件格式)
- [配置项](#配置项options)
- [公开 API 说明与示例](#公开-api-说明与示例)
- [并发模型](#并发模型)
- [构建](#构建)
- [测试体系](#测试体系)
- [基准测试](#基准测试--benchmark)
- [示例程序](#示例程序)

---

## 特性概览

| 特性 | 说明 |
|------|------|
| Append-only 写入 | 所有写操作只追加到活跃 segment，不修改已有数据 |
| 内存哈希索引 | `key → (file_id, offset, size)` 全量保存在内存，点查 O(1) |
| 多 segment 自动轮转 | 活跃文件达到 `max_data_file_size` 后自动创建新文件 |
| Merge / Compaction | 合并多个 segment，清理旧版本和墓碑记录，减少磁盘占用 |
| Hint 文件 | Merge 后输出 `hint_<id>.hint`，加速重启索引重建 |
| Index Snapshot | 关闭时持久化 `index.snapshot`，加速冷启动，避免全量扫描 |
| CRC32 完整性校验 | 每条记录写入时计算并存储 CRC32，读取时校验，防止静默数据损坏 |
| 崩溃恢复 | 支持截断末尾不完整记录，自动从最后一个完整状态恢复 |
| 锁分层并发控制 | 分离 `index_mutex` / `append_mutex` / `files_mutex`，读路径先取索引快照后再读文件 |
| Group Commit | 写请求进入队列由后台 writer 批量追加，`sync_on_write=true` 时按批次 fsync |
| 可选后台任务线程 | 后台定时 Sync / Merge，可按需开启 |
| 丰富的操作接口 | `Put` / `Get` / `Delete` / `WriteBatch` / `Scan` / `Fold` / `Iterator` |
| Raft 领导者选举模块（新增） | 提供 Follower/Candidate/Leader 状态机、随机选举超时、投票与心跳处理逻辑 |

---

## 架构设计

```
┌─────────────────────────────────────────────────────┐
│                    应用层 / 用户代码                  │
└──────────────────────────┬──────────────────────────┘
                           │ Put / Get / Delete / ...
                           ▼
┌─────────────────────────────────────────────────────┐
│                      KVStore                         │
│  ┌───────────────────────────────────────────────┐  │
│  │           内存索引 (index_)                    │  │
│  │   std::unordered_map<string, IndexEntry>       │  │
│  │   key → { file_id, offset, record_size, ... }  │  │
│  └───────────────────────────────────────────────┘  │
│  ┌──────────────┐   ┌────────────────────────────┐  │
│  │ 后台任务线程  │   │  文件管理                   │  │
│  │ (Sync/Merge) │   │  data_files_: id→DataFile   │  │
│  └──────────────┘   └────────────────────────────┘  │
└──────────────────────────┬──────────────────────────┘
                           │ Append / Read / Sync
                           ▼
┌─────────────────────────────────────────────────────┐
│                     DataFile                         │
│  负责单个 segment 文件的编解码、追加写、随机读、Sync   │
└──────────────────────────┬──────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
     data_1.log      data_2.log     data_N.log (active)
     [不可变]         [不可变]       [当前活跃，追加写]
         │
    hint_1.hint      index.snapshot
    (Merge产出)      (关闭时写出)
```

### 写入路径（含 Group Commit）

1. 用户调用 `Put(key, value)` 或 `Delete(key)`。
2. 写入先进入内部队列，由单 writer 线程按批次处理（保证 WAL 顺序）。
3. writer 线程串行 append：必要时 rotate，再执行 `DataFile::Append()`。
4. 对同一批次统一执行一次 `Sync()`（当 `sync_on_write == true`）。
5. 批次内逐条更新内存索引 `index_[key] = IndexEntry{...}`。

### 读取路径

1. 用户调用 `Get(key, &value)`。
2. 先在 `index_mutex` 保护下读取 `IndexEntry` 快照并立即释放索引锁。
3. 再在 `files_mutex` 保护下取得 `shared_ptr<DataFile>`，释放锁后调用 `Read(offset)`。
4. DataFile 读取固定头部（25 字节）解析 key/value 大小，再读取 payload，校验 CRC。
5. 返回 value。

### 一致性语义

- **Read Committed（默认）**：`Get()` 读取已完成 append + 索引更新的最新提交数据。
- **Linearizable（可选）**：`GetWithConsistency(key, value, ReadConsistency::kLinearizable)` 会先等待已入队写请求完成，再执行读取，确保经过写序列点。

### 恢复路径（重启时）

```
Open()
  ├── 扫描目录，发现所有 data_*.log
  ├── TryLoadIndexSnapshot()
  │     ├── 读取 index.snapshot
  │     ├── 校验 magic、version、文件元数据（大小 + mtime）
  │     └── 若匹配 → 直接加载索引，跳过全量扫描 ✓
  └── (若 snapshot 失效) Recover()
        ├── 按 file_id 升序遍历每个 segment
        ├── 非活跃 segment → 先尝试加载 hint_<id>.hint
        │     └── 若 hint 存在且有效 → 从 hint 重建索引（更快）
        ├── 活跃 segment / hint 不存在 → 逐条扫描 segment
        ├── 遇到尾部不完整记录 → Truncate() 并继续下一个 segment
        └── 遇到 CRC/格式损坏 → 返回错误
```

---

## 项目结构

```text
kvdb/
├── CMakeLists.txt          — 构建配置
├── README.md               — 本文档
├── include/                — 公共头文件
│   ├── data_file.h         — DataFile 类声明（segment 文件抽象）
│   ├── index_entry.h       — IndexEntry 结构体（内存索引项）
│   ├── kv_store.h          — KVStore 主类声明（公开 API）
│   ├── log_record.h        — LogRecord 结构体 + RecordType 枚举
│   ├── options.h           — Options 配置结构体
│   ├── status.h            — Status 错误码类
│   └── raft_leader_election.h — Raft 领导者选举状态机
├── src/                    — 实现文件
│   ├── data_file.cpp       — DataFile 实现（编解码、CRC、读写、Sync）
│   ├── kv_store.cpp        — KVStore 实现（索引、恢复、Merge、后台任务）
│   ├── raft_leader_election.cpp — Raft 选举状态机实现（任期、投票、心跳）
│   └── main.cpp            — 最小演示程序
└── tests/
    ├── kv_store_test.cpp   — 功能、边界、崩溃恢复、完整性测试
    ├── kv_store_benchmark.cpp — Put/Get 吞吐量基准测试
    ├── kv_store_perf_platform.cpp — 可配置性能测试平台（支持 CSV 结果）
    └── raft_leader_election_test.cpp — Raft 领导者选举状态机测试
```

---

## 核心模块说明

### `KVStore`（`include/kv_store.h` / `src/kv_store.cpp`）

数据库主入口，对外暴露全部用户 API。主要职责：

- **生命周期管理**：`Open()` / `Close()`，包括目录创建、文件发现、索引恢复和快照写出。
- **读写操作**：`Put` / `Get` / `Delete` / `WriteBatch` / `Scan` / `Fold` / `NewIterator`。
- **Rotate**：写入时检查活跃文件是否超限，自动滚动到新 segment。
- **Merge/Compaction**：将所有存活 key 重写到单一新 segment，同时产出 hint 文件。
- **索引持久化**：`SaveIndexSnapshot()` / `TryLoadIndexSnapshot()`。
- **后台任务**：`StartBackgroundWorker()` 启动定时 Sync/Merge 线程。

### `DataFile`（`include/data_file.h` / `src/data_file.cpp`）

单个 segment 文件的抽象封装。主要职责：

- **文件管理**：`Open(writable)` / `Close()` / `Sync()` / `Truncate()`。
- **写入**：`Append(record, &offset, &size)` — 序列化 → seekp 到末尾 → 写入。
- **读取**：`Read(offset, &record, &size)` — 两次读取（先头后体）→ 解码 → CRC 校验。
- **编解码**：`EncodeRecord()` / `DecodeRecord()`，实现二进制格式的序列化/反序列化。
- **CRC32**：使用 Castagnoli 多项式（ISO 3309），覆盖 type/timestamp/key/value。

### `LogRecord` + `RecordType`（`include/log_record.h`）

磁盘日志的逻辑表示：

- `kPut`：写入/更新操作，包含非空 key 和任意 value。
- `kDelete`：软删除操作，仅包含 key，value 为空。

### `IndexEntry`（`include/index_entry.h`）

内存哈希索引（`std::unordered_map<string, IndexEntry>`）中的值类型：

| 字段 | 含义 |
|------|------|
| `file_id` | 对应的 segment 文件编号 |
| `offset` | 记录在文件中的起始字节偏移 |
| `record_size` | 整条编码记录的字节大小 |
| `value_size` | value 的字节大小 |
| `key_size` | key 的字节大小 |
| `timestamp` | 写入时间戳（Unix 秒） |
| `record_type` | kPut / kDelete |
| `tombstone` | 是否为墓碑（预留扩展） |

### `Status`（`include/status.h`）

统一的函数返回值类型，用于传递成功/失败信息：

| 错误码 | 含义 |
|--------|------|
| `kOk` | 操作成功 |
| `kNotFound` | key 不存在 |
| `kIOError` | 底层 I/O 失败 |
| `kOutOfRange` | 偏移/长度越界 |
| `kCorruption` | 数据格式损坏 |
| `kChecksumFailed` | CRC 校验失败 |
| `kInvalidArgument` | 非法参数 |

### `Options`（`include/options.h`）

构建 KVStore 时传入的配置项，详见[配置项](#配置项options)章节。

---

## 磁盘文件格式

### segment 数据文件（`data_<id>.log`）

每条记录的二进制布局（小端序）：

```
┌──────────┬────────┬───────────┬──────────┬───────────┬────────┐
│ magic    │ type   │ timestamp │ key_size │ value_size│  crc   │
│  4 字节  │ 1 字节 │  8 字节   │  4 字节  │  4 字节   │ 4 字节 │
├──────────┴────────┴───────────┴──────────┴───────────┴────────┤
│                    key（key_size 字节）                         │
├───────────────────────────────────────────────────────────────┤
│                    value（value_size 字节）                     │
└───────────────────────────────────────────────────────────────┘
 固定头部 = 25 字节（magic 4 + type 1 + timestamp 8 + key_size 4 + value_size 4 + crc 4）
```

- **magic**：`0x4B564443`（"KVDC"），用于快速识别文件格式，不纳入 CRC。
- **crc**：覆盖 `type | timestamp | key_size | value_size | key | value`。

### hint 文件（`hint_<id>.hint`）

Merge 操作后产出，仅记录存活 key 的位置，不含 value，用于加速恢复：

```
[magic: 4B][version: 4B][file_id: 4B][entry_count: 4B]
对每条记录：
  [key_size: 4B][key: key_size B]
  [offset: 8B][record_size: 4B][value_size: 4B][key_size: 4B]
  [timestamp: 8B][record_type: 1B]
```

- **magic**：`0x4B564848`（"KVHH"）。

### index snapshot 文件（`index.snapshot`）

每次 `Close()` 或 `Merge()` 后写出，记录所有 IndexEntry，用于跳过全量扫描：

```
[magic: 4B][version: 4B][file_count: 4B][entry_count: 4B][active_file_id: 4B]
对每个 segment：
  [file_id: 4B][file_size: 8B][mtime_ticks: 8B]
对每条索引：
  [key_size: 4B][key: key_size B]
  [file_id: 4B][offset: 8B][record_size: 4B][value_size: 4B][key_size: 4B]
  [timestamp: 8B][record_type: 1B][tombstone: 1B]
```

- **magic**：`0x4B565350`（"KVSP"）。
- 加载时会校验每个 segment 的 `file_size` 和 `mtime`；任一不符则放弃 snapshot，回退到 `Recover()`。

---

## 配置项（`Options`）

```cpp
struct Options {
    std::string db_path = "./data";              // 数据库目录
    bool sync_on_write = false;                  // 每次写入后立即 fsync
    std::size_t max_data_file_size = 64 << 20;  // 单 segment 上限（默认 64 MB）
    bool enable_background_sync = false;         // 开启后台定时 Sync
    std::size_t background_sync_interval_ms = 1000;    // 后台 Sync 间隔（ms）
    bool enable_background_merge = false;        // 开启后台定时 Merge
    std::size_t background_merge_interval_ms = 10000;  // 后台 Merge 间隔（ms）
    std::size_t background_merge_min_segments = 2;     // 触发 Merge 的最小 segment 数
};
```

---

## 公开 API 说明与示例

### 打开与关闭

```cpp
#include "kv_store.h"

Options opt;
opt.db_path = "./mydb";
opt.sync_on_write = false;

KVStore db(opt);

// 打开数据库（首次创建目录，后续恢复索引）
Status s = db.Open();
assert(s.ok());

// 使用完毕后关闭（持久化 index snapshot）
s = db.Close();
assert(s.ok());
```

### Put / Get / Delete

```cpp
// 写入
s = db.Put("username", "alice");
s = db.Put("score", "9527");

// 读取
std::string value;
s = db.Get("username", &value);
if (s.ok()) {
    std::cout << "username = " << value << "\n";  // username = alice
}

// 覆盖写
s = db.Put("score", "10000");

// 删除（写入 tombstone，不立即释放磁盘空间）
s = db.Delete("score");

// 读取已删除的 key
s = db.Get("score", &value);
assert(s.code() == Status::kNotFound);
```

### WriteBatch（批量写入）

```cpp
std::vector<KVStore::WriteBatchOp> ops = {
    {RecordType::kPut,    "key1", "value1"},
    {RecordType::kPut,    "key2", "value2"},
    {RecordType::kDelete, "old_key", ""},
};

s = db.WriteBatch(ops);
assert(s.ok());
```

> **注意**：当前 WriteBatch 不是原子事务——任一操作失败后，已执行的操作不会回滚。

### 前缀扫描（Scan）

```cpp
// 写入一批带前缀的 key
db.Put("user:001", "Alice");
db.Put("user:002", "Bob");
db.Put("user:003", "Carol");
db.Put("order:001", "Order1");

// 扫描所有 "user:" 前缀的 key（最多返回 10 条，0 表示不限）
std::vector<std::pair<std::string, std::string>> result;
s = db.Scan("user:", 10, &result);
// result = [("user:001","Alice"), ("user:002","Bob"), ("user:003","Carol")]
```

### 遍历迭代器（Iterator）

```cpp
std::unique_ptr<KVStore::Iterator> iter;
s = db.NewIterator(&iter);
assert(s.ok());

for (; iter->Valid(); iter->Next()) {
    std::cout << iter->Key() << " -> " << iter->Value() << "\n";
}
```

> 迭代器是创建时刻的**快照**，创建后的写入不影响迭代结果。

### Fold（遍历回调）

```cpp
s = db.Fold([](const std::string& key, const std::string& value) -> Status {
    std::cout << key << " = " << value << "\n";
    return Status::OK();
    // 若返回非 OK，遍历立即中止并将该状态向上传递
});
```

### Merge / Compaction

```cpp
// 手动触发合并：清理旧版本和 tombstone，磁盘上只保留最新值
s = db.Merge();
assert(s.ok());
```

### 开启后台自动任务

```cpp
Options opt;
opt.db_path = "./mydb";
opt.enable_background_sync = true;
opt.background_sync_interval_ms = 2000;   // 每 2 秒 Sync 一次
opt.enable_background_merge = true;
opt.background_merge_interval_ms = 60000; // 每 60 秒尝试 Merge
opt.background_merge_min_segments = 3;    // 至少 3 个 segment 才 Merge

KVStore db(opt);
db.Open();
// 后台线程已启动，无需手动调用
```

---

## 并发模型

KVStore 内部使用 `std::shared_mutex` 实现读写锁：

| 操作 | 锁类型 | 说明 |
|------|--------|------|
| `Get` | 共享锁（`std::shared_lock`） | 多个读者可并发执行 |
| `Put` / `Delete` / `WriteBatch` | 独占锁（`std::unique_lock`） | 写入互斥，阻塞读者 |
| `Open` / `Close` / `Merge` | 独占锁 | 生命周期操作互斥 |
| 后台 Sync / Merge | 独占锁 | 与前台写入互斥 |

> **注意**：`WriteBatch` 对多条操作整体持有一次独占锁，但不提供 ACID 事务保证。
> 若中途失败，已写入的操作**不会**自动回滚。

---

## 构建

```bash
cmake -S . -B build
cmake --build build
```

默认生成三个可执行文件：

| 文件 | 说明 |
|------|------|
| `build/bin/kvdb` | 最小演示程序 |
| `build/bin/kvdb_test` | 功能 + 崩溃恢复 + 完整性测试 |
| `build/bin/kvdb_bench` | 吞吐量基准测试（固定规模） |
| `build/bin/kvdb_perf` | 可配置性能测试平台（建议用于回归） |

---

## 测试体系

### 1. 功能与边界测试

`kv_store_test.cpp` 覆盖：

- 基础读写、覆盖写、删除、不存在 key
- 空 key / 空 value
- Scan 的 limit 边界（`limit=1`、`limit=0`）
- WriteBatch 混合写入和非法输入
- Iterator / Fold / prefix scan 行为

### 2. 崩溃恢复测试

覆盖以下典型场景：

- 正常关闭后重启恢复
- 多版本覆盖后恢复
- 删除后恢复
- segment 尾部半条记录（文件截断）恢复
- 旧 segment 损坏后继续恢复后续 segment
- snapshot 损坏时回退到日志扫描恢复

### 3. 数据完整性测试

覆盖：

- bad magic
- bad record type
- CRC mismatch
- 越界读取错误码

### 4. 压力测试

- 大量随机写删后重启
- 与内存期望结果逐 key 对比

```bash
./build/bin/kvdb_test
```

---

## 基准测试 / Benchmark

### 1) 快速基准（固定规模）

```bash
./build/bin/kvdb_bench
```

默认执行固定规模 Put/Get（10 万次）并输出 Put/Get 总耗时与估算 QPS。

### 2) 性能测试平台（推荐）

```bash
./build/bin/kvdb_perf --profile mixed --ops 200000 --threads 1 \
  --out ./testdata/perf/results.csv
```

`kvdb_perf` 支持：

- **场景模板**：`mixed` / `write-heavy` / `read-heavy`
- **参数化压测**：操作数、线程数、value 大小、预填充 key 数、sync 策略
- **结果落盘**：追加写入 CSV，方便新功能合入前后做对比
- **延迟分位数**：输出 p50 / p95 / p99（微秒）

常用示例：

```bash
# 写密集（适合评估写路径改动）
./build/bin/kvdb_perf --profile write-heavy --ops 300000 --threads 1

# 读密集 + 更大 value（适合评估索引/读取优化）
./build/bin/kvdb_perf --profile read-heavy --value-size 512 --threads 1

# 保留历史数据目录，不清理，观察长期运行趋势
./build/bin/kvdb_perf --profile mixed --no-clean --out ./testdata/perf/results.csv
```

也可以使用脚本一次性执行回归场景：

```bash
# 一键执行回归基线场景（写密集/混合/读密集）
./scripts/run_perf_regression.sh
```

---

## 示例程序

```bash
./build/bin/kvdb
```

示例流程：`Open` → `Put("name", "bitcask-cpp")` → `Put("lang", "c++17")` → `Get("name")` → `Delete("lang")` → `Get("lang")` → `Close`。

期望输出：

```
name = bitcask-cpp
Get(lang): NotFound: key deleted
```
