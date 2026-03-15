#pragma once

#include <cstdint>

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

    // 写入时间戳
    uint64_t timestamp = 0;

    // 是否 tombstone
    // 第一版 index_ 中通常不保留已删除 key，这个字段主要为后续扩展准备
    bool tombstone = false;
};