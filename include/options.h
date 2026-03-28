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

    // 单个数据文件的最大大小
    // 第一版虽然还没做 rotation，但先预留配置位
    std::size_t max_data_file_size = 64 << 20;  // 64MB
};
