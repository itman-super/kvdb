#pragma once

#include <cstdint>

#include "log_record.h"

// IndexEntry 表示内存索引中的一个条目。
// Bitcask 风格核心思想：
// - 磁盘上只追加写
// - 内存中保存 key -> value 所在位置的映射
struct IndexEntry {
    // 记录所属文件编号
    // 第一版只有一个文件，但提前预留 file_id，便于后续扩成多 segment
    uint32_t file_id = 0;

    // 该 key 最新版本记录在文件中的偏移
    uint64_t offset = 0;

    // 整条 record 的字节大小
    // 后续做扫描、merge、调试时会有帮助
    uint32_t record_size = 0;

    // value 的字节大小
    uint32_t value_size = 0;

    // key 的字节大小
    // 主要用于：
    //  - 更快估算重写/merge 的写放大成本
    //  - 调试时快速校验 key/value 元数据是否一致
    uint32_t key_size = 0;

    // 写入时间戳
    uint64_t timestamp = 0;

    // 该索引条目对应的记录类型（通常是 kPut）。
    // 目前 index_ 默认不保留 tombstone，但在持久化/调试场景下保留类型信息，
    // 为后续扩展（例如墓碑索引、TTL 索引）提供更丰富元数据。
    RecordType record_type = RecordType::kPut;

    // 是否 tombstone
    // 第一版 index_ 中通常不保留已删除 key，这个字段主要为后续扩展准备
    bool tombstone = false;
};
