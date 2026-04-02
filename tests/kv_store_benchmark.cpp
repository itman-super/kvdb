#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>

#include "kv_store.h"

namespace fs = std::filesystem;

static void PrepareDir(const std::string& path) {
    std::error_code ec;
    fs::remove_all(path, ec);
    fs::create_directories(path, ec);
}

/// @brief 简单 benchmark：统计固定操作数下的 Put/Get 吞吐与耗时。
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
