#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "data_file.h"
#include "index_entry.h"
#include "log_record.h"
#include "options.h"
#include "status.h"

// KVStore 是对外暴露的数据库主类。
// 第一版实现：
// - 单 active data file
// - 内存哈希索引
// - 启动扫描日志做恢复
class KVStore {
public:
    explicit KVStore(Options options);
    ~KVStore();

    // 打开数据库：
    // - 创建目录
    // - 打开日志文件
    // - 扫描历史日志并恢复内存索引
    Status Open();

    // 关闭数据库
    Status Close();

    // 写入/覆盖 key
    Status Put(const std::string& key, const std::string& value);

    // 读取 key
    Status Get(const std::string& key, std::string* value);

    // 删除 key
    // 语义上会写入 tombstone，再从内存索引移除
    Status Delete(const std::string& key);

private:
    // 启动恢复
    // 顺序扫描日志文件，重建 index_
    Status Recover();

    // 内部统一追加日志记录，并回填 IndexEntry
    Status AppendRecord(const LogRecord& record, IndexEntry* entry);

private:
    Options options_;

    // 当前活跃数据文件
    std::unique_ptr<DataFile> active_file_;

    // 内存索引：key -> 最新记录位置
    std::unordered_map<std::string, IndexEntry> index_;

    // 当前 active file 的文件号
    uint32_t active_file_id_ = 1;

    // 数据库是否已打开
    bool opened_ = false;
};