#pragma once

#include <cstddef>
#include <string>

// Options 表示数据库的配置项。
// 第一版先保留最核心的几个选项，后续可以继续扩展：
// - 是否启用 fsync
// - segment 文件大小
// - merge 触发阈值
// - cache 配置
struct Options {
    // 数据库目录
    std::string db_path = "./data";

    // 每次写入后是否立即执行 durable sync（flush + fsync/fdatasync）
    // 默认 false：换取吞吐；开启后单条写入延迟会显著上升，但崩溃后丢失窗口更小。
    bool sync_on_write = false;

    // 单个数据文件(segment)的最大大小。
    // 到达阈值后会自动创建新的 data_<id>.log。
    std::size_t max_data_file_size = 64 << 20;  // 64MB

    // 是否开启后台线程定期对 active segment 执行 Sync。
    bool enable_background_sync = false;

    // 后台 Sync 间隔（毫秒），仅在 enable_background_sync=true 时生效。
    std::size_t background_sync_interval_ms = 1000;

    // 是否开启后台线程定期执行 Merge。
    bool enable_background_merge = false;

    // 后台 Merge 间隔（毫秒），仅在 enable_background_merge=true 时生效。
    std::size_t background_merge_interval_ms = 10000;

    // 触发后台 Merge 的最小 segment 数（包含 active segment）。
    // 例如设置为 2，表示至少有两个 data_*.log 文件时才会尝试 Merge。
    std::size_t background_merge_min_segments = 2;
};
