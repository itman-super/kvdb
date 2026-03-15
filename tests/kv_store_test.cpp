#include <filesystem>
#include <iostream>
#include <string>

#include "C:/Users/HONOR/Desktop/DB/include/kv_store.h"

namespace fs = std::filesystem;

// 这里实现一个最小测试框架，避免第一版就引入 gtest 依赖。
// 核心思想：
// - 每个测试函数独立准备目录
// - 用断言宏校验行为
// - 最后统一汇总测试结果
static int g_passed = 0;
static int g_failed = 0;

// 断言表达式为真
#define ASSERT_TRUE(expr)                                                     \
    do {                                                                      \
        if (!(expr)) {                                                        \
            std::cerr << "[FAILED] " << __FUNCTION__                          \
                      << " | ASSERT_TRUE(" #expr ") at line " << __LINE__     \
                      << std::endl;                                           \
            ++g_failed;                                                       \
            return;                                                           \
        }                                                                     \
    } while (0)

// 断言两个值相等
#define ASSERT_EQ(lhs, rhs)                                                   \
    do {                                                                      \
        auto _lhs = (lhs);                                                    \
        auto _rhs = (rhs);                                                    \
        if (!(_lhs == _rhs)) {                                                \
            std::cerr << "[FAILED] " << __FUNCTION__                          \
                      << " | ASSERT_EQ(" #lhs ", " #rhs ") at line "          \
                      << __LINE__ << ", lhs=" << _lhs << ", rhs=" << _rhs     \
                      << std::endl;                                           \
            ++g_failed;                                                       \
            return;                                                           \
        }                                                                     \
    } while (0)

// 断言返回状态为 OK
#define ASSERT_STATUS_OK(status_expr)                                         \
    do {                                                                      \
        Status _s = (status_expr);                                            \
        if (!_s.ok()) {                                                       \
            std::cerr << "[FAILED] " << __FUNCTION__                          \
                      << " | ASSERT_STATUS_OK(" #status_expr ") at line "     \
                      << __LINE__ << ", status=" << _s.ToString()             \
                      << std::endl;                                           \
            ++g_failed;                                                       \
            return;                                                           \
        }                                                                     \
    } while (0)

// 断言返回状态码符合预期
#define ASSERT_STATUS_CODE(status_expr, expected_code)                        \
    do {                                                                      \
        Status _s = (status_expr);                                            \
        if (_s.code() != (expected_code)) {                                   \
            std::cerr << "[FAILED] " << __FUNCTION__                          \
                      << " | ASSERT_STATUS_CODE(" #status_expr ", "           \
                      << #expected_code << ") at line " << __LINE__           \
                      << ", got=" << _s.ToString()                            \
                      << std::endl;                                           \
            ++g_failed;                                                       \
            return;                                                           \
        }                                                                     \
    } while (0)

// 记录测试通过
static void PassTest(const std::string& name) {
    std::cout << "[PASSED] " << name << std::endl;
    ++g_passed;
}

// 清理测试目录，保证测试相互隔离
static void CleanDir(const std::string& path) {
    std::error_code ec;
    fs::remove_all(path, ec);
    fs::create_directories(path, ec);
}

// 统一构造测试配置
static Options MakeOptions(const std::string& path) {
    Options opt;
    opt.db_path = path;
    opt.sync_on_write = false;
    return opt;
}

// 测试最基本的写入和读取
void TestPutAndGet() {
    const std::string path = "./testdata/test_put_get";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_OK(db.Put("name", "kvdb"));
    ASSERT_STATUS_OK(db.Put("lang", "cpp"));

    std::string value;
    ASSERT_STATUS_OK(db.Get("name", &value));
    ASSERT_EQ(value, std::string("kvdb"));

    ASSERT_STATUS_OK(db.Get("lang", &value));
    ASSERT_EQ(value, std::string("cpp"));

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

// 测试同一个 key 多次写入后，是否以最后一次为准
void TestOverwrite() {
    const std::string path = "./testdata/test_overwrite";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_OK(db.Put("k1", "v1"));
    ASSERT_STATUS_OK(db.Put("k1", "v2"));
    ASSERT_STATUS_OK(db.Put("k1", "v3"));

    std::string value;
    ASSERT_STATUS_OK(db.Get("k1", &value));
    ASSERT_EQ(value, std::string("v3"));

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

// 测试删除后不可再读
void TestDelete() {
    const std::string path = "./testdata/test_delete";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_OK(db.Put("k1", "v1"));
    ASSERT_STATUS_OK(db.Delete("k1"));

    std::string value;
    ASSERT_STATUS_CODE(db.Get("k1", &value), Status::kNotFound);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

// 测试删除不存在的 key
void TestDeleteNonExistentKey() {
    const std::string path = "./testdata/test_delete_non_existent";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_CODE(db.Delete("not_exist"), Status::kNotFound);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

// 测试关闭再打开后，数据是否能恢复
void TestRecoveryAfterReopen() {
    const std::string path = "./testdata/test_recovery_reopen";
    CleanDir(path);

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());

        ASSERT_STATUS_OK(db.Put("name", "bitcask-cpp"));
        ASSERT_STATUS_OK(db.Put("version", "v1"));
        ASSERT_STATUS_OK(db.Close());
    }

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());

        std::string value;
        ASSERT_STATUS_OK(db.Get("name", &value));
        ASSERT_EQ(value, std::string("bitcask-cpp"));

        ASSERT_STATUS_OK(db.Get("version", &value));
        ASSERT_EQ(value, std::string("v1"));

        ASSERT_STATUS_OK(db.Close());
    }

    PassTest(__FUNCTION__);
}

// 测试“覆盖写”在重启恢复后是否依旧正确
void TestRecoveryWithOverwrite() {
    const std::string path = "./testdata/test_recovery_overwrite";
    CleanDir(path);

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());

        ASSERT_STATUS_OK(db.Put("k", "v1"));
        ASSERT_STATUS_OK(db.Put("k", "v2"));
        ASSERT_STATUS_OK(db.Put("k", "v3"));
        ASSERT_STATUS_OK(db.Close());
    }

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());

        std::string value;
        ASSERT_STATUS_OK(db.Get("k", &value));
        ASSERT_EQ(value, std::string("v3"));

        ASSERT_STATUS_OK(db.Close());
    }

    PassTest(__FUNCTION__);
}

// 测试删除在重启恢复后是否依旧生效
void TestRecoveryWithDelete() {
    const std::string path = "./testdata/test_recovery_delete";
    CleanDir(path);

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());

        ASSERT_STATUS_OK(db.Put("k1", "v1"));
        ASSERT_STATUS_OK(db.Put("k2", "v2"));
        ASSERT_STATUS_OK(db.Delete("k1"));
        ASSERT_STATUS_OK(db.Close());
    }

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());

        std::string value;
        ASSERT_STATUS_CODE(db.Get("k1", &value), Status::kNotFound);

        ASSERT_STATUS_OK(db.Get("k2", &value));
        ASSERT_EQ(value, std::string("v2"));

        ASSERT_STATUS_OK(db.Close());
    }

    PassTest(__FUNCTION__);
}

// 测试空 key 的非法输入
void TestEmptyKey() {
    const std::string path = "./testdata/test_empty_key";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_CODE(db.Put("", "value"), Status::kInvalidArgument);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

// 测试读取不存在的 key
void TestGetNonExistentKey() {
    const std::string path = "./testdata/test_get_non_existent";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    std::string value;
    ASSERT_STATUS_CODE(db.Get("missing", &value), Status::kNotFound);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

int main() {
    TestPutAndGet();
    TestOverwrite();
    TestDelete();
    TestDeleteNonExistentKey();
    TestRecoveryAfterReopen();
    TestRecoveryWithOverwrite();
    TestRecoveryWithDelete();
    TestEmptyKey();
    TestGetNonExistentKey();

    std::cout << "\n========== TEST SUMMARY ==========" << std::endl;
    std::cout << "PASSED: " << g_passed << std::endl;
    std::cout << "FAILED: " << g_failed << std::endl;

    if (g_failed != 0) {
        return 1;
    }
    return 0;
}