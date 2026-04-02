#pragma once

#include <cstddef>
#include <string>

/// @brief 数据库配置项，在构造 KVStore 时传入，不支持运行时动态修改。
///
/// 设计原则：
///  - 提供合理的默认值，零配置即可使用。
///  - 关键性能参数（sync_on_write、max_data_file_size）可按需调整。
///  - 后台线程选项默认关闭，避免隐式引入并发开销。
///
/// 使用示例：
/// @code
///   Options opt;
///   opt.db_path = "/var/lib/myapp/kvdb";
///   opt.sync_on_write = true;
///   opt.max_data_file_size = 128 << 20;  // 128 MB
///   KVStore db(opt);
/// @endcode
struct Options {
    /// @brief 数据库存储目录（默认为当前工作目录下的 `./data`）。
    ///
    /// 若目录不存在，Open() 时会自动创建。
    /// 该目录下会生成以下文件：
    ///  - `data_<id>.log`    — segment 数据文件
    ///  - `hint_<id>.hint`   — Merge 产出的轻量索引文件
    ///  - `index.snapshot`   — 内存索引快照（加速启动）
    std::string db_path = "./data";

    /// @brief 每次写入后是否立即执行持久化同步（flush + fsync/fdatasync）。
    ///
    /// - `false`（默认）：写入仅到达操作系统内核缓冲区，吞吐量更高，
    ///   但进程崩溃或断电时可能丢失最近若干条写入。
    /// - `true`：每条写入都会等待数据落盘，durability 最强，
    ///   但单条写入延迟会显著上升（取决于磁盘速度）。
    bool sync_on_write = false;

    /// @brief 单个 segment 文件（`data_<id>.log`）的大小上限（字节）。
    ///
    /// 当活跃文件大小 + 本次写入大小超过此阈值时，
    /// 系统会自动将当前活跃文件封存，并创建新的 segment。
    /// 设置为 0 表示不限制（仅使用一个 segment，不自动 rotate）。
    /// 默认值：64 MiB。
    std::size_t max_data_file_size = 64 << 20;  // 64MB

    /// @brief 是否开启后台线程定期对活跃 segment 执行 Sync。
    ///
    /// 开启后，后台线程会每隔 background_sync_interval_ms 毫秒
    /// 调用一次 active_file_->Sync()，在不阻塞写入的前提下减少数据丢失窗口。
    bool enable_background_sync = false;

    /// @brief 后台定时 Sync 的间隔（毫秒）。
    ///
    /// 仅在 enable_background_sync == true 时生效。默认 1000 ms（1 秒）。
    std::size_t background_sync_interval_ms = 1000;

    /// @brief 是否开启后台线程定期执行 Merge/Compaction。
    ///
    /// 开启后，后台线程会每隔 background_merge_interval_ms 毫秒检查一次
    /// segment 数量，若满足条件则自动触发 Merge，清理旧版本和墓碑记录。
    bool enable_background_merge = false;

    /// @brief 后台定时 Merge 的间隔（毫秒）。
    ///
    /// 仅在 enable_background_merge == true 时生效。默认 10000 ms（10 秒）。
    std::size_t background_merge_interval_ms = 10000;

    /// @brief 触发后台 Merge 的最小 segment 数（含活跃 segment）。
    ///
    /// 例如设置为 2，表示至少存在两个 `data_*.log` 文件时才触发 Merge，
    /// 避免数据量极少时频繁执行无意义的合并操作。
    std::size_t background_merge_min_segments = 2;
};
