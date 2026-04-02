// include/status.h
#pragma once

#include <string>
#include <utility>

/// @brief 统一表达函数执行结果的状态类。
///
/// 存储引擎中常见的设计模式：相比直接抛异常，Status 更适合底层组件
/// 跨调用栈逐层传递错误信息，同时保留了细粒度的错误分类，
/// 方便调用方针对不同错误类型进行差异化处理。
///
/// 使用示例：
/// @code
///   Status s = db.Put("key", "value");
///   if (!s.ok()) {
///       std::cerr << s.ToString() << std::endl;
///   }
/// @endcode
class Status {
public:
    /// @brief 错误码枚举，用于区分不同类型的失败原因。
    enum Code {
        kOk = 0,            ///< 操作成功。
        kNotFound,          ///< 目标 key 不存在于数据库中。
        kIOError,           ///< 底层文件读写错误（如磁盘满、权限不足）。
        kOutOfRange,        ///< 读取偏移或长度越出文件范围。
        kCorruption,        ///< 数据损坏：magic 不符、类型非法、长度异常等。
        kChecksumFailed,    ///< CRC32 校验失败，记录内容被篡改或损坏。
        kInvalidArgument    ///< 调用方传入了非法参数（如空 key、null 指针）。
    };

    /// @brief 默认构造，表示操作成功（kOk）。
    Status() : code_(kOk), msg_("OK") {}

    /// @brief 构造指定错误码和错误消息的 Status 对象。
    /// @param code 错误码。
    /// @param msg  描述错误原因的字符串。
    Status(Code code, std::string msg) : code_(code), msg_(std::move(msg)) {}

    /// @name 静态工厂方法
    /// 便于调用方快速构造各类 Status，无需重复写 new Status(...)。
    ///@{

    /// @brief 构造表示成功的 Status。
    static Status OK() { return Status(); }

    /// @brief 构造 kNotFound 状态。
    /// @param msg 可选的详细说明（如 "key not found"）。
    static Status NotFound(std::string msg = "") { return Status(kNotFound, std::move(msg)); }

    /// @brief 构造 kIOError 状态。
    /// @param msg 可选的详细说明（如 "write failed"）。
    static Status IOError(std::string msg = "") { return Status(kIOError, std::move(msg)); }

    /// @brief 构造 kOutOfRange 状态。
    /// @param msg 可选的详细说明（如 "offset out of range"）。
    static Status OutOfRange(std::string msg = "") { return Status(kOutOfRange, std::move(msg)); }

    /// @brief 构造 kCorruption 状态。
    /// @param msg 可选的详细说明（如 "bad magic"）。
    static Status Corruption(std::string msg = "") { return Status(kCorruption, std::move(msg)); }

    /// @brief 构造 kChecksumFailed 状态。
    /// @param msg 可选的详细说明（如 "crc mismatch"）。
    static Status ChecksumFailed(std::string msg = "") { return Status(kChecksumFailed, std::move(msg)); }

    /// @brief 构造 kInvalidArgument 状态。
    /// @param msg 可选的详细说明（如 "key is empty"）。
    static Status InvalidArgument(std::string msg = "") { return Status(kInvalidArgument, std::move(msg)); }
    ///@}

    /// @brief 判断操作是否成功（code == kOk）。
    /// @return 成功返回 true，否则返回 false。
    bool ok() const { return code_ == kOk; }

    /// @brief 获取当前错误码。
    /// @return 错误码枚举值。
    Code code() const { return code_; }

    /// @brief 获取错误消息字符串（原始文本，不含前缀）。
    /// @return 错误消息的常量引用。
    const std::string& message() const { return msg_; }

    /// @brief 将 Status 格式化为可读字符串（含错误类型前缀），便于日志打印和调试。
    ///
    /// 示例输出：
    ///   - `"OK"`
    ///   - `"NotFound: key not found"`
    ///   - `"IOError: write failed"`
    ///
    /// @return 格式化后的状态字符串。
    std::string ToString() const {
        switch (code_) {
            case kOk: return "OK";
            case kNotFound: return "NotFound: " + msg_;
            case kIOError: return "IOError: " + msg_;
            case kOutOfRange: return "OutOfRange: " + msg_;
            case kCorruption: return "Corruption: " + msg_;
            case kChecksumFailed: return "ChecksumFailed: " + msg_;
            case kInvalidArgument: return "InvalidArgument: " + msg_;
            default: return "Unknown";
        }
    }

private:
    Code code_;       ///< 错误码。
    std::string msg_; ///< 错误描述文本。
};