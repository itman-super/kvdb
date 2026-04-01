# kvdb

一个使用 **C++17** 实现的轻量级、Bitcask 风格键值存储原型。

项目当前采用 **append-only 日志 + 内存哈希索引** 的设计：写入只追加到日志末尾，读取通过内存索引定位偏移，启动时扫描日志恢复最新状态。它更适合学习和实验，不以生产可用性为目标。

## 特性概览

- 基于追加写的持久化日志
- 多 segment data file：`data_<id>.log`
- Merge 后生成 hint file：`hint_<id>.hint`
- 内存索引：`key -> IndexEntry`
- 关闭时持久化索引快照：`index.snapshot`
- 支持 `Put` / `Get` / `Delete`
- 支持幂等删除（删除不存在 key 返回 OK）
- 支持 `WriteBatch` 批量写入（put/delete 混合）
- 支持 `Iterator` / `Scan(prefix)` / `Fold` 遍历能力
- 删除使用 tombstone 语义
- 重启后可通过扫描日志恢复索引
- 若 snapshot 校验通过，优先从 `index.snapshot` 快速恢复
- 记录级校验
- 校验项包含 `magic` 和 `CRC32`
- 基础错误码封装：`OK`、`NotFound`、`IOError`、`OutOfRange`、`Corruption`、`ChecksumFailed`、`InvalidArgument`
- 能处理尾部半条记录：恢复时截断损坏尾部并保留前面的完整数据

## 适用场景

适合：

- 学习 Bitcask / append-only 存储结构
- 课程设计、实验项目、个人练手
- 演示日志恢复、校验和、tombstone 等基础机制

不适合：

- 生产环境持久化存储
- 高并发读写
- 对恢复一致性、性能、磁盘语义有严格要求的场景

## 项目结构

```text
kvdb/
├── CMakeLists.txt
├── README.md
├── data/
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
    └── kv_store_test.cpp
```

## 核心设计

### 1. 写入路径

`Put(key, value)` 的流程：

1. 组装一条 `kPut` 记录
2. 将记录追加写入日志文件末尾
3. 返回该记录的偏移与长度
4. 更新内存索引，使 key 指向最新记录

### 2. 读取路径

`Get(key, &value)` 的流程：

1. 先查询内存索引
2. 找到记录偏移后，从日志文件中按偏移读取完整记录
3. 校验 magic、记录类型和 CRC32
4. 返回 value

### 3. 删除语义

`Delete(key)` 不是原地删除，而是：

1. 追加一条 `kDelete` tombstone 记录
2. 从内存索引中移除该 key

因此删除状态在重启恢复后仍然成立。

### 4. Segment 轮转与启动恢复

写入时会根据 `max_data_file_size` 执行 rotation：

1. 若 `active segment` 大小 + 新记录大小超过阈值，则切换到新的 `data_<id+1>.log`
2. 老 segment 保持只读，供查询与恢复使用

数据库 `Open()` 时会调用 `Recover()`：

1. 扫描目录下全部 `data_<id>.log`，按 `id` 升序恢复
2. 逐条解析并校验记录
3. 遇到 `kPut`：更新索引
4. 遇到 `kDelete`：从索引中移除对应 key
5. 如果 active segment 尾部存在不完整记录：将其截断到最后一条完整记录后继续使用
6. 如果遇到真正的数据损坏或 CRC 失败：返回错误，不自动修复

## 日志记录格式

当前记录头格式如下：

```text
magic(4) + type(1) + timestamp(8) + key_size(4) + value_size(4) + crc(4)
```

其后紧跟：

```text
key bytes + value bytes
```

字段含义：

- `magic`：固定魔数，用于识别合法记录
- `type`：记录类型，当前支持 `kPut` 和 `kDelete`
- `timestamp`：写入时间戳
- `key_size` / `value_size`：key/value 长度
- `crc`：对 `type | timestamp | key_size | value_size | key | value` 计算的 CRC32

## 关键模块

### `KVStore`

对外暴露数据库接口，负责：

- `Open()` / `Close()`
- `Put()` / `Get()` / `Delete()`
- 启动时恢复内存索引

### `DataFile`

负责单个日志文件（segment）的底层读写：

- 打开与关闭文件
- 追加记录
- 按偏移读取记录
- `flush`
- 尾部截断

### `LogRecord`

表示一条逻辑记录，当前仅支持两种类型：

- `kPut`
- `kDelete`

### `IndexEntry`

表示内存索引项，保存：

- `file_id`（定位到具体 segment）
- `offset`
- `record_size`
- `value_size`
- `timestamp`
- `tombstone`

### `Status`

统一表达操作结果，避免底层组件直接抛异常，便于调用方判断错误类型。

## 配置项

`include/options.h` 中当前包含以下配置：

- `db_path`
  数据目录，默认 `./data`
- `sync_on_write`
  每次写入后是否立即 `flush`
- `max_data_file_size`
  单个 segment 的最大大小，超过后会自动 rotation

说明：`sync_on_write` 当前调用的是 `file_.flush()`，并不是严格意义上的 `fsync` / `fdatasync`。

## 构建要求

- CMake >= 3.16
- 支持 C++17 的编译器
- GCC / Clang / MSVC 任一即可

## 构建项目

### Windows PowerShell

```powershell
cmake -S . -B build
cmake --build build
```

### Linux / macOS

```bash
cmake -S . -B build
cmake --build build
```

默认会生成两个可执行文件：

- `kvdb`
- `kvdb_test`

输出目录由 `CMakeLists.txt` 指定为：

```text
build/bin/
```

## 运行示例程序

主程序位于 `src/main.cpp`，演示流程包括：

- 打开数据库
- 写入 `name`、`lang`
- 读取 `name`
- 删除 `lang`
- 再次读取 `lang`
- 关闭数据库

运行方式：

### Windows PowerShell

```powershell
.\build\bin\kvdb.exe
```

### Linux / macOS

```bash
./build/bin/kvdb
```

首次运行后，会在数据库目录下生成类似文件：

```text
./data/data_1.log
./data/data_2.log
...
```

## 作为库使用

```cpp
#include "kv_store.h"

int main() {
    Options options;
    options.db_path = "./data";
    options.sync_on_write = false;

    KVStore db(options);

    Status s = db.Open();
    if (!s.ok()) {
        return 1;
    }

    s = db.Put("hello", "world");
    if (!s.ok()) {
        return 1;
    }

    std::string value;
    s = db.Get("hello", &value);
    if (!s.ok()) {
        return 1;
    }

    s = db.Delete("hello");
    if (!s.ok()) {
        return 1;
    }

    return db.Close().ok() ? 0 : 1;
}
```

## 测试覆盖

`tests/kv_store_test.cpp` 目前覆盖了以下场景：

- 基础写入 / 读取
- 同 key 覆盖写
- 删除已存在 key
- 删除不存在 key
- 重启后的恢复
- 覆盖写后的恢复
- 删除后的恢复
- 空 key 非法输入
- 读取不存在 key
- 大 value 的恢复
- magic 损坏检测
- CRC 校验失败检测
- 非法记录类型检测
- 越界读取返回 `OutOfRange`
- 尾部半条记录恢复与截断
- 多 segment rotation 与重启恢复

运行测试：

### Windows PowerShell

```powershell
.\build\bin\kvdb_test.exe
```

### Linux / macOS

```bash
./build/bin/kvdb_test
```

## 当前限制

这是一个第一版原型，当前仍有明显限制：

- 暂未实现 merge / compaction，历史 segment 只增不减
- 覆盖写和删除会持续累积历史记录，日志只会增长
- 内存索引全部常驻内存，没有容量控制
- 没有并发控制，不具备线程安全保证
- `sync_on_write` 仅 `flush`，未提供更强磁盘持久化语义
- 暂无迭代器、批量写、事务、快照、压缩、TTL 等高级能力

## 后续可扩展方向

如果继续完善，比较自然的方向有：

- merge / compaction
- hint file，加快恢复速度
- 更强的持久化控制
- 并发读写支持
- 批量写入
- 迭代器与范围扫描
- TTL / 过期淘汰
- 更规范的测试框架，例如 GoogleTest
- 更完善的指标、日志和错误处理

## 小结

`kvdb` 当前已经具备一个最小 Bitcask 风格存储原型的核心闭环：

- 日志追加写
- 内存索引定位
- tombstone 删除
- 重启恢复
- 基础完整性校验

如果你的目标是理解键值存储的基础结构，这个项目已经是一个清晰、可继续扩展的起点。
