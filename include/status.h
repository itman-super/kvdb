#pragma once

#include <string>
#include <utility>

// Status 用于统一表达函数执行结果。
// 这是存储引擎里非常常见的设计：相比直接抛异常，Status 更适合底层组件层层返回错误。
class Status {
public:
    // 错误码定义
    enum Code {
        kOk = 0,            // 成功
        kNotFound,          // key 不存在
        kIOError,           // 文件读写错误
        kCorruption,        // 数据损坏、日志格式异常
        kInvalidArgument    // 非法参数
    };

    // 默认构造表示 OK
    Status() : code_(kOk), msg_("OK") {}

    // 构造指定状态
    Status(Code code, std::string msg) : code_(code), msg_(std::move(msg)) {}

    // 一组静态工厂函数，便于调用方快速构造结果
    static Status OK() { return Status(); }
    static Status NotFound(std::string msg = "") { return Status(kNotFound, std::move(msg)); }
    static Status IOError(std::string msg = "") { return Status(kIOError, std::move(msg)); }
    static Status Corruption(std::string msg = "") { return Status(kCorruption, std::move(msg)); }
    static Status InvalidArgument(std::string msg = "") { return Status(kInvalidArgument, std::move(msg)); }

    // 是否成功
    bool ok() const { return code_ == kOk; }

    // 获取错误码
    Code code() const { return code_; }

    // 获取错误消息
    const std::string& message() const { return msg_; }

    // 格式化输出，便于打印日志和调试
    std::string ToString() const {
        switch (code_) {
            case kOk: return "OK";
            case kNotFound: return "NotFound: " + msg_;
            case kIOError: return "IOError: " + msg_;
            case kCorruption: return "Corruption: " + msg_;
            case kInvalidArgument: return "InvalidArgument: " + msg_;
            default: return "Unknown";
        }
    }

private:
    Code code_;
    std::string msg_;
};