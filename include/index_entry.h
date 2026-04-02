#pragma once

#include <cstdint>

#include "log_record.h"

/// @brief 内存索引中的单个条目（Bitcask 风格 keydir 的最小单元）。
///
/// Bitcask 核心思想：
///  - 磁盘上所有写操作均为 append-only。
///  - 内存中维护一张哈希表 `key -> IndexEntry`，记录每个 key 最新版本
///    在磁盘上的精确位置，从而实现 O(1) 级别的点查。
///
/// 当一个 key 被多次写入时，每次写入都会更新对应的 IndexEntry，
/// 旧版本数据仍留在磁盘，直到 Merge/Compaction 时才会被清除。
struct IndexEntry {
    /// @brief 记录所属 segment 文件的文件编号（file_id）。
    ///
    /// 引入 file_id 字段是为了支持多 segment 扩展：
    /// 当活跃文件超过大小阈值后会自动 rotate，
    /// 索引需要通过 file_id 定位到正确的 DataFile 实例。
    uint32_t file_id = 0;

    /// @brief 该 key 最新版本记录在 segment 文件中的起始偏移（字节）。
    ///
    /// 读取时使用此偏移直接调用 DataFile::Read(offset, ...)，
    /// 无需顺序扫描文件，实现 O(1) 随机读。
    uint64_t offset = 0;

    /// @brief 整条序列化记录的字节大小（头部 + key + value）。
    ///
    /// 在 Merge/Compaction 和崩溃恢复扫描时用于步进偏移：
    /// `next_offset = offset + record_size`。
    uint32_t record_size = 0;

    /// @brief value 的字节大小。
    ///
    /// 主要用于估算存储空间占用和 Merge 后的写放大成本。
    uint32_t value_size = 0;

    /// @brief key 的字节大小。
    ///
    /// 用于快速估算 Merge 写放大成本，以及调试时校验 key/value 元数据一致性。
    uint32_t key_size = 0;

    /// @brief 写入时间戳（Unix 秒级，与对应 LogRecord::timestamp 相同）。
    ///
    /// 当前版本主要用于观测和调试，后续可扩展为 TTL 或多版本索引依据。
    uint64_t timestamp = 0;

    /// @brief 该索引条目对应的记录类型（通常为 kPut）。
    ///
    /// 活跃索引（index_）中正常情况下只保留 kPut 条目；
    /// 保留此字段是为后续扩展（如墓碑索引、TTL 索引）提供更丰富的元数据。
    RecordType record_type = RecordType::kPut;

    /// @brief 标记该条目是否为墓碑（tombstone）。
    ///
    /// 当前版本 index_ 中不保留已删除 key 的条目（直接 erase），
    /// 此字段主要为后续扩展（延迟删除、软删除索引）预留。
    bool tombstone = false;
};
