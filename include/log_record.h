#pragma once

#include <cstdint>
#include <string>

/// @brief 日志记录类型枚举。
///
/// 用于区分一条 LogRecord 是正常的写入操作，还是代表删除的墓碑记录。
/// - kPut    → 插入或更新（写入 key-value 对）
/// - kDelete → 软删除（写入墓碑记录，value 为空）
enum class RecordType : uint8_t {
    kPut = 1,    ///< 写入/更新操作。
    kDelete = 2  ///< 删除操作（写入 tombstone，value 为空）。
};

/// @brief 追加写日志（append-only log）中的逻辑记录。
///
/// 所有写操作（Put / Delete）最终都会被转换为一条 LogRecord，
/// 通过 DataFile::Append() 序列化并持久化到 segment 文件。
///
/// 磁盘二进制格式（由 DataFile 负责编解码）：
/// @code
///   [magic: 4B][type: 1B][timestamp: 8B][key_size: 4B][value_size: 4B][crc: 4B]
///   [key: key_size B][value: value_size B]
/// @endcode
struct LogRecord {
    /// @brief 记录类型（kPut 或 kDelete）。
    RecordType type = RecordType::kPut;

    /// @brief 写入时的逻辑时间戳（Unix 秒级时间）。
    ///
    /// 当前版本主要用于观测与调试，恢复时不依赖时间戳来判断记录新旧
    /// （顺序重放天然保证后写的记录覆盖前写的版本）。
    uint64_t timestamp = 0;

    /// @brief 记录的键（不得为空字符串）。
    std::string key;

    /// @brief 记录的值。
    /// 对于 kDelete 类型的墓碑记录，value 为空字符串。
    std::string value;
};