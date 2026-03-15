#pragma once

#include <cstdint>
#include <string>

// 记录类型：
// kPut    -> 插入或更新
// kDelete -> 删除（写 tombstone）
enum class RecordType : uint8_t {
    kPut = 1,
    kDelete = 2
};

// LogRecord 是 append-only 日志中的逻辑记录。
// 第一版里所有写操作都会转换成一条 LogRecord 追加到磁盘。
struct LogRecord {
    // 记录类型
    RecordType type = RecordType::kPut;

    // 逻辑时间戳
    // 当前版本主要用于观测与后续扩展，第一版恢复不依赖它判新旧
    uint64_t timestamp = 0;

    // key
    std::string key;

    // value
    // 若是 Delete 记录，则 value 为空
    std::string value;
};