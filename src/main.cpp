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