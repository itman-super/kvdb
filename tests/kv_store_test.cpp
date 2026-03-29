// tests/kv_store_test.cpp
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "data_file.h"
#include "kv_store.h"

namespace fs = std::filesystem;

static int g_passed = 0;
static int g_failed = 0;

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

static void PassTest(const std::string& name) {
    std::cout << "[PASSED] " << name << std::endl;
    ++g_passed;
}

static void CleanDir(const std::string& path) {
    std::error_code ec;
    fs::remove_all(path, ec);
    fs::create_directories(path, ec);
}

static Options MakeOptions(const std::string& path) {
    Options opt;
    opt.db_path = path;
    opt.sync_on_write = false;
    return opt;
}

static std::string DataFilePath(const std::string& db_path, uint32_t file_id = 1) {
    return db_path + "/data_" + std::to_string(file_id) + ".log";
}

// 把文件某个位置的一个字节翻转，用于模拟磁盘数据损坏。
// 这比直接覆盖成固定值更稳，因为基本能保证 CRC 改变。
static bool FlipOneByte(const std::string& file_path, std::streamoff offset) {
    std::fstream file(file_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        return false;
    }

    file.seekg(0, std::ios::end);
    std::streamoff file_size = file.tellg();
    if (offset < 0 || offset >= file_size) {
        return false;
    }

    file.seekg(offset, std::ios::beg);
    char ch = 0;
    file.read(&ch, 1);
    if (!file) {
        return false;
    }

    ch ^= 0x01;

    file.clear();
    file.seekp(offset, std::ios::beg);
    file.write(&ch, 1);
    return static_cast<bool>(file);
}

// 截断文件末尾若干字节，用于模拟“尾部半条记录”。
// Windows 下 std::filesystem::resize_file 可直接用。
static bool TruncateFileTail(const std::string& file_path, std::uintmax_t bytes_to_remove) {
    std::error_code ec;
    std::uintmax_t size = fs::file_size(file_path, ec);
    if (ec || size <= bytes_to_remove) {
        return false;
    }

    fs::resize_file(file_path, size - bytes_to_remove, ec);
    return !ec;
}

void TestPutAndGet() {
    const std::string path = "./testdata/test_put_get_crc";
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

void TestOverwrite() {
    const std::string path = "./testdata/test_overwrite_crc";
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

void TestDelete() {
    const std::string path = "./testdata/test_delete_crc";
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

void TestDeleteNonExistentKey() {
    const std::string path = "./testdata/test_delete_non_existent_crc";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_CODE(db.Delete("not_exist"), Status::kNotFound);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

void TestRecoveryAfterReopen() {
    const std::string path = "./testdata/test_recovery_reopen_crc";
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

void TestRecoveryWithOverwrite() {
    const std::string path = "./testdata/test_recovery_overwrite_crc";
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

void TestRecoveryWithDelete() {
    const std::string path = "./testdata/test_recovery_delete_crc";
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

void TestEmptyKey() {
    const std::string path = "./testdata/test_empty_key_crc";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_CODE(db.Put("", "value"), Status::kInvalidArgument);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

void TestGetNonExistentKey() {
    const std::string path = "./testdata/test_get_non_existent_crc";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    std::string value;
    ASSERT_STATUS_CODE(db.Get("missing", &value), Status::kNotFound);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

// 新增：验证 CRC 正常路径下，大 value 也可以正确恢复。
void TestRecoveryWithLargeValue() {
    const std::string path = "./testdata/test_large_value_crc";
    CleanDir(path);

    std::string large_value(4096, 'x');

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("blob", large_value));
        ASSERT_STATUS_OK(db.Close());
    }

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());

        std::string value;
        ASSERT_STATUS_OK(db.Get("blob", &value));
        ASSERT_EQ(value, large_value);

        ASSERT_STATUS_OK(db.Close());
    }

    PassTest(__FUNCTION__);
}

// 新增：手工破坏 magic 字段，期望下次 Open() 时恢复失败，返回 Corruption（格式损坏）。
void TestCRCDetectsCorruptionOnOpen() {
    const std::string path = "./testdata/test_crc_detect_open";
    CleanDir(path);

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("k1", "hello"));
        ASSERT_STATUS_OK(db.Put("k2", "world"));
        ASSERT_STATUS_OK(db.Close());
    }

    const std::string file_path = DataFilePath(path);

    // 改动文件开头的 magic 字段，触发 bad magic。
    ASSERT_TRUE(FlipOneByte(file_path, 0));

    {
        KVStore db(MakeOptions(path));
        Status s = db.Open();
        ASSERT_TRUE(!s.ok());
        ASSERT_EQ(s.code(), Status::kCorruption);
    }

    PassTest(__FUNCTION__);
}

// 新增：模拟尾部半条记录。
// 当前 Recover() 里如果遇到 IOError，会 break 并停止恢复。
// 所以预期是：Open() 仍然成功，且至少前面的完整记录仍可读。
void TestRecoveryStopsAtPartialTailRecord() {
    const std::string path = "./testdata/test_partial_tail_crc";
    CleanDir(path);

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("k1", "v1"));
        ASSERT_STATUS_OK(db.Put("k2", "v2"));
        ASSERT_STATUS_OK(db.Close());
    }

    const std::string file_path = DataFilePath(path);

    // 截掉末尾几个字节，模拟最后一条记录不完整。
    ASSERT_TRUE(TruncateFileTail(file_path, 3));

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());

        std::string value;
        ASSERT_STATUS_OK(db.Get("k1", &value));
        ASSERT_EQ(value, std::string("v1"));

        // k2 可能因为最后一条记录损坏而恢复不到。
        Status s = db.Get("k2", &value);
        ASSERT_TRUE(s.ok() || s.code() == Status::kNotFound);

        ASSERT_STATUS_OK(db.Close());
    }

    PassTest(__FUNCTION__);
}

void TestMultiSegmentRotationAndRecovery() {
    const std::string path = "./testdata/test_multi_segment_rotation";
    CleanDir(path);

    Options opt = MakeOptions(path);
    opt.max_data_file_size = 64;

    {
        KVStore db(opt);
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("k1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        ASSERT_STATUS_OK(db.Put("k2", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        ASSERT_STATUS_OK(db.Put("k3", "cccccccccccccccccccccccccccccc"));
        ASSERT_STATUS_OK(db.Close());
    }

    ASSERT_TRUE(fs::exists(DataFilePath(path, 1)));
    ASSERT_TRUE(fs::exists(DataFilePath(path, 2)));

    {
        KVStore db(opt);
        ASSERT_STATUS_OK(db.Open());

        std::string value;
        ASSERT_STATUS_OK(db.Get("k1", &value));
        ASSERT_EQ(value, std::string("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        ASSERT_STATUS_OK(db.Get("k2", &value));
        ASSERT_EQ(value, std::string("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        ASSERT_STATUS_OK(db.Get("k3", &value));
        ASSERT_EQ(value, std::string("cccccccccccccccccccccccccccccc"));
        ASSERT_STATUS_OK(db.Close());
    }

    PassTest(__FUNCTION__);
}

void TestOpenFailureReturnsIOError() {
    const std::string bad_path = "./testdata/not_exist_dir/data_1.log";
    std::error_code ec;
    fs::remove_all("./testdata/not_exist_dir", ec);

    // 只读打开不存在文件：预期返回 IOError（不会自动创建）。
    DataFile file(1, bad_path);
    Status s = file.Open(false);
    ASSERT_EQ(s.code(), Status::kIOError);
    PassTest(__FUNCTION__);
}

void TestReadOutOfRangeReturnsOutOfRange() {
    const std::string path = "./testdata/test_read_out_of_range";
    CleanDir(path);
    const std::string file_path = DataFilePath(path);

    DataFile file(1, file_path);
    ASSERT_STATUS_OK(file.Open(true));

    LogRecord rec;
    rec.type = RecordType::kPut;
    rec.timestamp = 1;
    rec.key = "k";
    rec.value = "v";

    uint64_t offset = 0;
    uint32_t size = 0;
    ASSERT_STATUS_OK(file.Append(rec, &offset, &size));

    LogRecord out;
    uint32_t out_size = 0;
    // 构造越界 offset，验证返回 kOutOfRange。
    Status s = file.Read(offset + size + 1, &out, &out_size);
    ASSERT_EQ(s.code(), Status::kOutOfRange);
    ASSERT_STATUS_OK(file.Close());
    PassTest(__FUNCTION__);
}

void TestChecksumFailureReturnsChecksumFailed() {
    const std::string path = "./testdata/test_checksum_failed_code";
    CleanDir(path);

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("k1", "hello"));
        ASSERT_STATUS_OK(db.Close());
    }

    const std::string file_path = DataFilePath(path);
    // 修改 value 区域一个字节，触发 CRC mismatch。
    ASSERT_TRUE(FlipOneByte(file_path, 26));

    {
        KVStore db(MakeOptions(path));
        Status s = db.Open();
        ASSERT_TRUE(!s.ok());
        ASSERT_EQ(s.code(), Status::kChecksumFailed);
    }

    PassTest(__FUNCTION__);
}

void TestInvalidRecordTypeReturnsCorruption() {
    const std::string path = "./testdata/test_invalid_record_type";
    CleanDir(path);

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("k1", "hello"));
        ASSERT_STATUS_OK(db.Close());
    }

    const std::string file_path = DataFilePath(path);
    // type 字段位于 magic(4) 之后；翻转后应触发 bad record type。
    ASSERT_TRUE(FlipOneByte(file_path, 4));

    {
        KVStore db(MakeOptions(path));
        Status s = db.Open();
        ASSERT_TRUE(!s.ok());
        ASSERT_EQ(s.code(), Status::kCorruption);
    }

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

    TestRecoveryWithLargeValue();
    TestCRCDetectsCorruptionOnOpen();
    TestRecoveryStopsAtPartialTailRecord();
    TestMultiSegmentRotationAndRecovery();
    TestOpenFailureReturnsIOError();
    TestReadOutOfRangeReturnsOutOfRange();
    TestChecksumFailureReturnsChecksumFailed();
    TestInvalidRecordTypeReturnsCorruption();

    std::cout << "\n========== TEST SUMMARY ==========" << std::endl;
    std::cout << "PASSED: " << g_passed << std::endl;
    std::cout << "FAILED: " << g_failed << std::endl;

    if (g_failed != 0) {
        return 1;
    }
    return 0;
}
