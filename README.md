# kvdb

一个使用 **C++17** 实现的轻量级、Bitcask 风格键值存储原型。

项目采用 **append-only 日志 + 内存哈希索引** 设计：写入只追加到日志末尾，读取通过内存索引定位偏移，启动时扫描日志（或加载快照/hint）恢复最新状态。它适合学习与实验，不以生产可用性为目标。

## 特性概览

- 追加写日志：`data_<id>.log`
- 多 segment 自动轮转（`max_data_file_size`）
- Merge/Compaction 后生成 `hint_<id>.hint`
- 关闭时持久化 `index.snapshot`
- 记录级完整性校验（magic + CRC32）
- 崩溃恢复：支持尾部半条记录截断恢复
- 基础并发控制：读写锁（读共享 / 写独占）
- 可选后台线程：定时 `Sync` / `Merge`
- 基础接口：`Put` / `Get` / `Delete` / `WriteBatch` / `Scan` / `Fold` / `Iterator`

## 项目结构

```text
kvdb/
├── CMakeLists.txt
├── README.md
├── include/
│   ├── data_file.h
│   ├── index_entry.h
│   ├── kv_store.h
│   ├── log_record.h
│   ├── options.h
│   └── status.h
├── src/
│   ├── data_file.cpp
│   ├── kv_store.cpp
│   └── main.cpp
└── tests/
    ├── kv_store_test.cpp
    └── kv_store_benchmark.cpp
```

## 核心模块说明

- `KVStore`：数据库主入口，负责 Open/Close、读写 API、恢复、merge、后台任务。
- `DataFile`：单个 segment 的编码、追加、读取、sync、truncate。
- `LogRecord`：逻辑日志记录（`kPut` / `kDelete`）。
- `IndexEntry`：内存索引项（`file_id + offset + meta`）。
- `Status`：统一错误码与错误文本。

> 目前已为关键公开 API 增加统一注释（Doxygen 风格），方便 IDE 跳转和二次开发。

## 配置项（`include/options.h`）

- `db_path`：数据库目录（默认 `./data`）
- `sync_on_write`：每次写入后是否同步落盘
- `max_data_file_size`：单个 segment 大小上限
- `enable_background_sync` / `background_sync_interval_ms`
- `enable_background_merge` / `background_merge_interval_ms`
- `background_merge_min_segments`

## 构建

```bash
cmake -S . -B build
cmake --build build
```

默认生成：

- `build/bin/kvdb`：示例程序
- `build/bin/kvdb_test`：测试程序
- `build/bin/kvdb_bench`：基准程序

## 测试体系（已增强）

### 1) 功能与边界测试

`kv_store_test.cpp` 覆盖：

- 基础读写、覆盖写、删除、不存在 key
- 空 key / 空 value
- Scan 的 limit 边界（`limit=1`、`limit=0`）
- WriteBatch 混合写入和非法输入
- Iterator / Fold / prefix scan 行为

### 2) 崩溃恢复测试

覆盖以下典型场景：

- 正常关闭后重启恢复
- 多版本覆盖后恢复
- 删除后恢复
- segment 尾部半条记录（文件截断）恢复
- 旧 segment 损坏后继续恢复后续 segment
- snapshot 损坏时回退到日志扫描恢复

### 3) 数据完整性测试

覆盖：

- bad magic
- bad record type
- CRC mismatch
- 越界读取错误码

### 4) 压力测试

测试程序中增加随机 Put/Delete 压力恢复用例：

- 大量随机写删后重启
- 与内存期望结果逐 key 对比

运行测试：

```bash
./build/bin/kvdb_test
```

## 基准测试 / Benchmark

运行：

```bash
./build/bin/kvdb_bench
```

默认执行固定规模 Put/Get（10 万次）并输出：

- Put 总耗时和估算 QPS
- Get 总耗时和估算 QPS

> 该 benchmark 主要用于回归对比，不代表生产场景性能。

## 示例程序

```bash
./build/bin/kvdb
```

示例流程：Open -> Put -> Get -> Delete -> Get(NotFound) -> Close。
