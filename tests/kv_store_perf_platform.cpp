// tests/kv_store_perf_platform.cpp
//
// KVStore 性能测试平台（可扩展 workload + 可落盘结果）。
//
// 目标：
//  1) 提供统一入口，后续新功能上线后可直接复用同一套基准流程做回归。
//  2) 支持通过命令行快速切换场景（写密集、读密集、混合负载）。
//  3) 输出结构化 CSV 结果，便于接入 CI、Excel 或可视化工具。
//
// 示例：
//   ./build/bin/kvdb_perf --profile write-heavy --ops 200000 --out ./testdata/perf/result.csv
//   ./build/bin/kvdb_perf --profile read-heavy --threads 4 --value-size 256

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "kv_store.h"

namespace fs = std::filesystem;

namespace {

struct PerfConfig {
    std::string profile = "mixed";
    std::string db_path = "./testdata/perf";
    std::string out_csv = "";

    std::size_t ops = 100000;
    std::size_t preload_keys = 20000;
    std::size_t value_size = 128;
    std::size_t threads = 1;

    // 百分比（0~100），剩余比例用于 Delete。
    int read_ratio = 50;
    int write_ratio = 45;

    std::uint32_t seed = 20260407;
    bool clean_dir = true;
    bool sync_on_write = false;
    std::size_t max_data_file_size = 8 << 20;
};

struct WorkerStat {
    std::size_t get_ok = 0;
    std::size_t get_not_found = 0;
    std::size_t get_fail = 0;
    std::size_t put_ok = 0;
    std::size_t put_fail = 0;
    std::size_t del_ok = 0;
    std::size_t del_fail = 0;
};

struct PerfResult {
    double elapsed_ms = 0.0;
    double qps = 0.0;
    double p50_us = 0.0;
    double p95_us = 0.0;
    double p99_us = 0.0;
    WorkerStat stat;
};

void PrintUsage(const char* prog) {
    std::cout
        << "KVStore performance platform\n"
        << "Usage: " << prog << " [options]\n\n"
        << "Options:\n"
        << "  --profile <mixed|write-heavy|read-heavy>  workload 模板\n"
        << "  --ops <N>                                 总操作数（默认 100000）\n"
        << "  --threads <N>                             并发线程数（默认 1）\n"
        << "  --preload-keys <N>                        预填充 key 数（默认 20000）\n"
        << "  --value-size <N>                          value 字节数（默认 128）\n"
        << "  --read-ratio <0-100>                      读操作占比\n"
        << "  --write-ratio <0-100>                     写操作占比\n"
        << "  --db-path <path>                          数据目录（默认 ./testdata/perf）\n"
        << "  --out <csv_path>                          结果追加写入 CSV\n"
        << "  --sync-on-write <0|1>                     是否每次写后 fsync\n"
        << "  --max-file-size <bytes>                   segment 轮转阈值\n"
        << "  --seed <N>                                随机种子\n"
        << "  --no-clean                                不清空已有目录\n"
        << "  --help                                    显示帮助\n";
}

bool ParseSizeT(const std::string& s, std::size_t* out) {
    if (out == nullptr || s.empty()) {
        return false;
    }
    try {
        const auto v = std::stoull(s);
        *out = static_cast<std::size_t>(v);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseInt(const std::string& s, int* out) {
    if (out == nullptr || s.empty()) {
        return false;
    }
    try {
        *out = std::stoi(s);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseArgs(int argc, char** argv, PerfConfig* cfg) {
    if (cfg == nullptr) {
        return false;
    }

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need_value = [&](const char* name) -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << name << std::endl;
                return "";
            }
            return std::string(argv[++i]);
        };

        if (arg == "--help") {
            PrintUsage(argv[0]);
            return false;
        }
        if (arg == "--profile") {
            const auto v = need_value("--profile");
            if (v.empty()) return false;
            cfg->profile = v;
            continue;
        }
        if (arg == "--ops") {
            if (!ParseSizeT(need_value("--ops"), &cfg->ops)) return false;
            continue;
        }
        if (arg == "--threads") {
            if (!ParseSizeT(need_value("--threads"), &cfg->threads)) return false;
            continue;
        }
        if (arg == "--preload-keys") {
            if (!ParseSizeT(need_value("--preload-keys"), &cfg->preload_keys)) return false;
            continue;
        }
        if (arg == "--value-size") {
            if (!ParseSizeT(need_value("--value-size"), &cfg->value_size)) return false;
            continue;
        }
        if (arg == "--read-ratio") {
            if (!ParseInt(need_value("--read-ratio"), &cfg->read_ratio)) return false;
            continue;
        }
        if (arg == "--write-ratio") {
            if (!ParseInt(need_value("--write-ratio"), &cfg->write_ratio)) return false;
            continue;
        }
        if (arg == "--db-path") {
            cfg->db_path = need_value("--db-path");
            if (cfg->db_path.empty()) return false;
            continue;
        }
        if (arg == "--out") {
            cfg->out_csv = need_value("--out");
            if (cfg->out_csv.empty()) return false;
            continue;
        }
        if (arg == "--sync-on-write") {
            std::size_t v = 0;
            if (!ParseSizeT(need_value("--sync-on-write"), &v)) return false;
            cfg->sync_on_write = (v != 0);
            continue;
        }
        if (arg == "--max-file-size") {
            if (!ParseSizeT(need_value("--max-file-size"), &cfg->max_data_file_size)) return false;
            continue;
        }
        if (arg == "--seed") {
            std::size_t v = 0;
            if (!ParseSizeT(need_value("--seed"), &v)) return false;
            cfg->seed = static_cast<std::uint32_t>(v);
            continue;
        }
        if (arg == "--no-clean") {
            cfg->clean_dir = false;
            continue;
        }

        std::cerr << "unknown argument: " << arg << std::endl;
        return false;
    }

    if (cfg->profile == "write-heavy") {
        cfg->read_ratio = 10;
        cfg->write_ratio = 85;
    } else if (cfg->profile == "read-heavy") {
        cfg->read_ratio = 90;
        cfg->write_ratio = 10;
    } else if (cfg->profile == "mixed") {
        // 使用用户显式参数或默认值。
    } else {
        std::cerr << "invalid profile: " << cfg->profile << std::endl;
        return false;
    }

    if (cfg->threads == 0 || cfg->ops == 0 || cfg->preload_keys == 0 || cfg->value_size == 0) {
        std::cerr << "ops/threads/preload-keys/value-size must be > 0" << std::endl;
        return false;
    }
    if (cfg->read_ratio < 0 || cfg->write_ratio < 0 || cfg->read_ratio > 100 ||
        cfg->write_ratio > 100 || cfg->read_ratio + cfg->write_ratio > 100) {
        std::cerr << "ratio must be in [0,100] and read+write<=100" << std::endl;
        return false;
    }
    return true;
}

void PrepareDir(const std::string& path, bool clean_dir) {
    std::error_code ec;
    if (clean_dir) {
        fs::remove_all(path, ec);
    }
    fs::create_directories(path, ec);
}

std::string BuildValue(std::size_t size, std::uint32_t seed, std::size_t i) {
    std::string value(size, 'x');
    const std::string tail = "_" + std::to_string(seed) + "_" + std::to_string(i);
    if (tail.size() <= value.size()) {
        std::copy(tail.begin(), tail.end(), value.end() - static_cast<std::ptrdiff_t>(tail.size()));
    }
    return value;
}

void ApplyStat(WorkerStat* all, const WorkerStat& one) {
    all->get_ok += one.get_ok;
    all->get_not_found += one.get_not_found;
    all->get_fail += one.get_fail;
    all->put_ok += one.put_ok;
    all->put_fail += one.put_fail;
    all->del_ok += one.del_ok;
    all->del_fail += one.del_fail;
}

double Percentile(std::vector<double> v, double p) {
    if (v.empty()) {
        return 0.0;
    }
    std::sort(v.begin(), v.end());
    const double rank = p * static_cast<double>(v.size() - 1);
    const std::size_t lo = static_cast<std::size_t>(rank);
    const std::size_t hi = std::min(v.size() - 1, lo + 1);
    const double frac = rank - static_cast<double>(lo);
    return v[lo] + frac * (v[hi] - v[lo]);
}

PerfResult RunPerf(const PerfConfig& cfg) {
    PerfResult result;

    PrepareDir(cfg.db_path, cfg.clean_dir);

    Options opt;
    opt.db_path = cfg.db_path;
    opt.sync_on_write = cfg.sync_on_write;
    opt.max_data_file_size = cfg.max_data_file_size;

    KVStore db(opt);
    Status s = db.Open();
    if (!s.ok()) {
        std::cerr << "Open failed: " << s.ToString() << std::endl;
        return result;
    }

    for (std::size_t i = 0; i < cfg.preload_keys; ++i) {
        s = db.Put("perf:key:" + std::to_string(i), BuildValue(cfg.value_size, cfg.seed, i));
        if (!s.ok()) {
            std::cerr << "Preload failed at " << i << ": " << s.ToString() << std::endl;
            (void)db.Close();
            return result;
        }
    }

    std::vector<std::thread> workers;
    std::vector<WorkerStat> per_thread(cfg.threads);
    std::vector<double> all_latency_us;
    std::mutex lat_mutex;

    const std::size_t ops_per_thread = cfg.ops / cfg.threads;
    const std::size_t remain_ops = cfg.ops % cfg.threads;

    auto start = std::chrono::steady_clock::now();

    for (std::size_t t = 0; t < cfg.threads; ++t) {
        const std::size_t my_ops = ops_per_thread + (t < remain_ops ? 1 : 0);
        workers.emplace_back([&, t, my_ops]() {
            std::mt19937 rng(cfg.seed + static_cast<std::uint32_t>(t * 7919));
            std::uniform_int_distribution<int> op_dis(0, 99);
            std::uniform_int_distribution<std::size_t> key_dis(0, cfg.preload_keys - 1);

            std::vector<double> local_lat;
            local_lat.reserve(my_ops);

            WorkerStat stat;
            std::string value;
            for (std::size_t i = 0; i < my_ops; ++i) {
                const int r = op_dis(rng);
                const std::size_t k = key_dis(rng);
                const std::string key = "perf:key:" + std::to_string(k);

                auto op_begin = std::chrono::steady_clock::now();
                if (r < cfg.read_ratio) {
                    Status st = db.Get(key, &value);
                    if (st.ok()) {
                        ++stat.get_ok;
                    } else if (st.code() == Status::kNotFound) {
                        ++stat.get_not_found;
                    } else {
                        ++stat.get_fail;
                    }
                } else if (r < cfg.read_ratio + cfg.write_ratio) {
                    Status st = db.Put(key, BuildValue(cfg.value_size, cfg.seed, i + t * my_ops));
                    if (st.ok()) {
                        ++stat.put_ok;
                    } else {
                        ++stat.put_fail;
                    }
                } else {
                    Status st = db.Delete(key);
                    if (st.ok()) {
                        ++stat.del_ok;
                    } else {
                        ++stat.del_fail;
                    }
                }
                auto op_end = std::chrono::steady_clock::now();
                const double lat = std::chrono::duration<double, std::micro>(op_end - op_begin).count();
                local_lat.push_back(lat);
            }

            {
                std::lock_guard<std::mutex> lk(lat_mutex);
                all_latency_us.insert(all_latency_us.end(), local_lat.begin(), local_lat.end());
            }
            per_thread[t] = stat;
        });
    }

    for (auto& th : workers) {
        th.join();
    }

    auto end = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration<double, std::milli>(end - start).count();

    result.elapsed_ms = elapsed;
    result.qps = elapsed > 0.0 ? (1000.0 * static_cast<double>(cfg.ops) / elapsed) : 0.0;
    result.p50_us = Percentile(all_latency_us, 0.50);
    result.p95_us = Percentile(all_latency_us, 0.95);
    result.p99_us = Percentile(all_latency_us, 0.99);

    for (const auto& s0 : per_thread) {
        ApplyStat(&result.stat, s0);
    }

    s = db.Close();
    if (!s.ok()) {
        std::cerr << "Close failed: " << s.ToString() << std::endl;
    }
    return result;
}

void PrintResult(const PerfConfig& cfg, const PerfResult& r) {
    std::cout << "==== kvdb perf result ====" << std::endl;
    std::cout << "profile      : " << cfg.profile << std::endl;
    std::cout << "ops          : " << cfg.ops << std::endl;
    std::cout << "threads      : " << cfg.threads << std::endl;
    std::cout << "preload_keys : " << cfg.preload_keys << std::endl;
    std::cout << "value_size   : " << cfg.value_size << std::endl;
    std::cout << "ratio        : read=" << cfg.read_ratio << "% write=" << cfg.write_ratio
              << "% delete=" << (100 - cfg.read_ratio - cfg.write_ratio) << "%" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "elapsed_ms   : " << r.elapsed_ms << std::endl;
    std::cout << "qps          : " << r.qps << std::endl;
    std::cout << "latency_us   : p50=" << r.p50_us << " p95=" << r.p95_us << " p99=" << r.p99_us
              << std::endl;
    std::cout << "get_ok/get_nf/get_fail : " << r.stat.get_ok << "/" << r.stat.get_not_found << "/"
              << r.stat.get_fail << std::endl;
    std::cout << "put_ok/put_fail        : " << r.stat.put_ok << "/" << r.stat.put_fail << std::endl;
    std::cout << "del_ok/del_fail        : " << r.stat.del_ok << "/" << r.stat.del_fail << std::endl;
}

void AppendCsv(const PerfConfig& cfg, const PerfResult& r) {
    if (cfg.out_csv.empty()) {
        return;
    }

    std::error_code ec;
    fs::create_directories(fs::path(cfg.out_csv).parent_path(), ec);

    const bool need_header = !fs::exists(cfg.out_csv, ec) || fs::file_size(cfg.out_csv, ec) == 0;
    std::ofstream out(cfg.out_csv, std::ios::app);
    if (!out.is_open()) {
        std::cerr << "failed to open csv: " << cfg.out_csv << std::endl;
        return;
    }

    if (need_header) {
        out << "profile,ops,threads,preload_keys,value_size,read_ratio,write_ratio,delete_ratio,"
            << "sync_on_write,max_file_size,elapsed_ms,qps,p50_us,p95_us,p99_us,"
            << "get_ok,get_not_found,get_fail,put_ok,put_fail,del_ok,del_fail\n";
    }

    out << cfg.profile << ',' << cfg.ops << ',' << cfg.threads << ',' << cfg.preload_keys << ','
        << cfg.value_size << ',' << cfg.read_ratio << ',' << cfg.write_ratio << ','
        << (100 - cfg.read_ratio - cfg.write_ratio) << ',' << (cfg.sync_on_write ? 1 : 0) << ','
        << cfg.max_data_file_size << ',' << std::fixed << std::setprecision(2) << r.elapsed_ms << ','
        << r.qps << ',' << r.p50_us << ',' << r.p95_us << ',' << r.p99_us << ',' << r.stat.get_ok
        << ',' << r.stat.get_not_found << ',' << r.stat.get_fail << ',' << r.stat.put_ok << ','
        << r.stat.put_fail << ',' << r.stat.del_ok << ',' << r.stat.del_fail << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    PerfConfig cfg;
    if (!ParseArgs(argc, argv, &cfg)) {
        return 1;
    }

    const PerfResult result = RunPerf(cfg);
    PrintResult(cfg, result);
    AppendCsv(cfg, result);
    return 0;
}
