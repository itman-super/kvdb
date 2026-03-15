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

    // 每次写入后是否立即 flush/sync
    // 第一版默认 false，追求先跑通逻辑，不追求最强持久化保证
    bool sync_on_write = false;

    // 单个数据文件的最大大小
    // 第一版虽然还没做 rotation，但先预留配置位
    std::size_t max_data_file_size = 64 << 20;  // 64MB
};