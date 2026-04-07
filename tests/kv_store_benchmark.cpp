// tests/kv_store_benchmark.cpp
//
// KVStore 吞吐量基准测试。
// 该程序通过对固定操作数（kOps = 100000）的 Put/Get 进行计时，
// 测量 KVStore 在关闭 sync_on_write 时的最大吞吐量，输出总耗时和估算 QPS。
//
// 结果仅用于回归对比，不代表生产场景性能。
// 运行方式：./build/bin/kvdb_bench
// 数据目录：./testdata/benchmark（测试开始前会清空该目录）

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>

#include "kv_store.h"

namespace fs = std::filesystem;

/// @brief 清空并重建指定目录，确保每次 benchmark 在干净的环境中运行。
/// @param path 要重建的目录路径；若已存在则先递归删除再创建。
static void PrepareDir(const std::string& path) {
    std::error_code ec;
    fs::remove_all(path, ec);
    fs::create_directories(path, ec);
}

/// @brief 简单 benchmark：统计固定操作数下的 Put/Get 吞吐与耗时。
///
/// 测试流程：
///  1. 创建干净的测试目录并打开数据库（max_data_file_size = 8 MB，关闭 sync_on_write）。
///  2. 执行 kOps 次 Put（"bench:key:<i>" -> "value:<i>"），计时。
///  3. 执行 kOps 次 Get（读取上一步写入的全部 key），计时。
///  4. 关闭数据库，打印 Put/Get 的耗时（ms）和估算 QPS。
///
/// @return 成功返回 0，任何操作失败返回 1。
int main() {
    const std::string path = "./testdata/benchmark";
    PrepareDir(path);

    Options opt;
    opt.db_path = path;
    opt.sync_on_write = false;
    opt.max_data_file_size = 8 << 20;

    KVStore db(opt);
    Status s = db.Open();
    if (!s.ok()) {
        std::cerr << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }

    constexpr int kOps = 100000;

    auto put_begin = std::chrono::steady_clock::now();
    for (int i = 0; i < kOps; ++i) {
        s = db.Put("bench:key:" + std::to_string(i), "value:" + std::to_string(i));
        if (!s.ok()) {
            std::cerr << "Put failed at " << i << ": " << s.ToString() << std::endl;
            return 1;
        }
    }
    auto put_end = std::chrono::steady_clock::now();

    std::string value;
    auto get_begin = std::chrono::steady_clock::now();
    for (int i = 0; i < kOps; ++i) {
        s = db.Get("bench:key:" + std::to_string(i), &value);
        if (!s.ok()) {
            std::cerr << "Get failed at " << i << ": " << s.ToString() << std::endl;
            return 1;
        }
    }
    auto get_end = std::chrono::steady_clock::now();

    s = db.Close();
    if (!s.ok()) {
        std::cerr << "Close failed: " << s.ToString() << std::endl;
        return 1;
    }

    const auto put_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(put_end - put_begin).count();
    const auto get_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(get_end - get_begin).count();

    const double put_qps = put_ms > 0 ? (1000.0 * kOps / static_cast<double>(put_ms)) : 0.0;
    const double get_qps = get_ms > 0 ? (1000.0 * kOps / static_cast<double>(get_ms)) : 0.0;

    std::cout << "kvdb benchmark (" << kOps << " ops)" << std::endl;
    std::cout << "Put: " << put_ms << " ms, QPS=" << put_qps << std::endl;
    std::cout << "Get: " << get_ms << " ms, QPS=" << get_qps << std::endl;
    return 0;
}
