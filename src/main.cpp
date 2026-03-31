// main.cpp — KVStore 最小演示程序。
//
// 该程序不是数据库的核心逻辑，主要用来手动验证以下操作的基本流程：
//  1. Open   — 打开（或创建）数据库目录并恢复索引
//  2. Put    — 写入两个键值对
//  3. Get    — 读取并打印 "name" 的值
//  4. Delete — 删除 "lang" 键（写入 tombstone）
//  5. Get    — 再次读取已删除的 "lang"，期望返回 NotFound
//  6. Close  — 关闭数据库并持久化 index snapshot
//
// 运行方式：./build/bin/kvdb
// 数据目录：./data（相对于当前工作目录）
#include <iostream>
#include <string>

#include "kv_store.h"

// main.cpp 只是一个最小演示程序。
// 它不是数据库的核心逻辑，主要用来验证：
// - Open
// - Put
// - Get
// - Delete
// - Close
int main() {
    Options options;
    options.db_path = "./data";
    options.sync_on_write = false;

    KVStore db(options);

    Status s = db.Open();
    if (!s.ok()) {
        std::cerr << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }

    s = db.Put("name", "bitcask-cpp");
    if (!s.ok()) {
        std::cerr << "Put(name) failed: " << s.ToString() << std::endl;
        return 1;
    }

    s = db.Put("lang", "c++17");
    if (!s.ok()) {
        std::cerr << "Put(lang) failed: " << s.ToString() << std::endl;
        return 1;
    }

    std::string value;
    s = db.Get("name", &value);
    if (s.ok()) {
        std::cout << "name = " << value << std::endl;
    } else {
        std::cerr << "Get(name) failed: " << s.ToString() << std::endl;
    }

    s = db.Delete("lang");
    if (!s.ok()) {
        std::cerr << "Delete(lang) failed: " << s.ToString() << std::endl;
        return 1;
    }

    s = db.Get("lang", &value);
    if (!s.ok()) {
        std::cout << "Get(lang): " << s.ToString() << std::endl;
    }

    s = db.Close();
    if (!s.ok()) {
        std::cerr << "Close failed: " << s.ToString() << std::endl;
        return 1;
    }

    return 0;
}