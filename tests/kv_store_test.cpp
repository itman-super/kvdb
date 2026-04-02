// tests/kv_store_test.cpp
//
// KVStore 集成测试。
// 采用轻量级的自定义断言宏（无需 Google Test 等第三方框架），
// 每个测试函数独立使用隔离目录，测试结束后统一汇报通过/失败数量。
//
// 测试分类：
//  - 基础功能：Put/Get/Delete、覆盖写、空 key、不存在 key 等。
//  - 崩溃恢复：重启后重建索引、多版本覆盖、删除后恢复等。
//  - 多 segment：segment rotate 及跨 segment 恢复。
//  - 数据完整性：CRC 检测、bad magic、bad record type、尾部半条记录截断等。
//  - Merge/Compaction：存活 key 保留、hint 文件生成、空库 merge 等。
//  - Index Snapshot：Close 时快照生成验证。

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

#include "data_file.h"
#include "kv_store.h"

namespace fs = std::filesystem;

// 全局测试计数器，main() 结束时汇总输出。
static int g_passed = 0;
static int g_failed = 0;

/// @brief 断言表达式为真，失败时打印位置信息、递增失败计数并从当前测试函数返回。
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

/// @brief 断言两个值相等，失败时同时打印两个值的实际内容。
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

/// @brief 断言 Status 为 OK，失败时打印具体错误信息。
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

/// @brief 断言 Status 的错误码等于 expected_code，失败时打印实际错误码。
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

/// @brief 标记测试通过，输出测试函数名并递增通过计数。
static void PassTest(const std::string& name) {
    std::cout << "[PASSED] " << name << std::endl;
    ++g_passed;
}

/// @brief 清空并重新创建测试目录，确保每个测试在干净的环境中运行。
static void CleanDir(const std::string& path) {
    std::error_code ec;
    fs::remove_all(path, ec);
    fs::create_directories(path, ec);
}

/// @brief 构造测试用 Options：使用指定路径，关闭每写 sync（提高测试速度）。
static Options MakeOptions(const std::string& path) {
    Options opt;
    opt.db_path = path;
    opt.sync_on_write = false;
    return opt;
}

/// @brief 构造指定目录和 file_id 的 segment 文件路径（测试辅助）。
static std::string DataFilePath(const std::string& db_path, uint32_t file_id = 1) {
    return db_path + "/data_" + std::to_string(file_id) + ".log";
}

/// @brief 构造指定目录和 file_id 的 hint 文件路径（测试辅助）。
static std::string HintFilePath(const std::string& db_path, uint32_t file_id) {
    return db_path + "/hint_" + std::to_string(file_id) + ".hint";
}

/// @brief 构造 index snapshot 文件路径（测试辅助）。
static std::string SnapshotPath(const std::string& db_path) {
    return db_path + "/index.snapshot";
}

/// @brief 统计目录下 data_*.log 文件的数量（测试辅助）。
/// 用于验证 segment 数量是否符合预期（如 rotate 或 merge 后）。
/// @return 文件数量，目录遍历失败时返回 -1。
static int CountDataFiles(const std::string& db_path) {
    std::error_code ec;
    int count = 0;
    for (const auto& entry : fs::directory_iterator(db_path, ec)) {
        if (ec) {
            return -1;
        }
        if (!entry.is_regular_file()) {
            continue;
        }
        const std::string name = entry.path().filename().string();
        if (name.rfind("data_", 0) == 0 && entry.path().extension() == ".log") {
            ++count;
        }
    }
    return count;
}

// 把文件某个位置的一个字节翻转，用于模拟磁盘数据损坏。
// 这比直接覆盖成固定值更稳，因为基本能保证 CRC 改变。
// @param file_path 要修改的文件路径。
// @param offset    要翻转的字节偏移（0 表示文件第一个字节）。
// @return 成功翻转返回 true，文件打开失败或偏移越界返回 false。
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

// 截断文件末尾若干字节，用于模拟“尾部半条记录”（进程崩溃时的典型场景）。
// Windows 下 std::filesystem::resize_file 可直接用。
// @param file_path      要截断的文件路径。
// @param bytes_to_remove 从文件末尾移除的字节数。
// @return 成功截断返回 true，文件不存在、大小不足或 resize 失败返回 false。
static bool TruncateFileTail(const std::string& file_path, std::uintmax_t bytes_to_remove) {
    std::error_code ec;
    std::uintmax_t size = fs::file_size(file_path, ec);
    if (ec || size <= bytes_to_remove) {
        return false;
    }

    fs::resize_file(file_path, size - bytes_to_remove, ec);
    return !ec;
}

/// @brief 基础读写测试：Put 两个 key，验证 Get 返回正确值，并正常 Close。
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

/// @brief 覆盖写测试：同一 key 写入三个版本，验证 Get 返回最新值 v3。
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

/// @brief 删除测试：Put 后 Delete，验证 Get 返回 kNotFound。
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

/// @brief 删除不存在 key 的测试：幂等删除应返回 OK。
void TestDeleteNonExistentKey() {
    const std::string path = "./testdata/test_delete_non_existent_crc";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_OK(db.Delete("not_exist"));

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

/// @brief 批量写入测试：验证 put/delete 混合批处理结果正确。
void TestWriteBatch() {
    const std::string path = "./testdata/test_write_batch";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    std::vector<KVStore::WriteBatchOp> ops = {
        {RecordType::kPut, "k1", "v1"},
        {RecordType::kPut, "k2", "v2"},
        {RecordType::kPut, "k1", "v3"},
        {RecordType::kDelete, "k2", ""},
        {RecordType::kDelete, "not_exist", ""},
    };

    ASSERT_STATUS_OK(db.WriteBatch(ops));

    std::string value;
    ASSERT_STATUS_OK(db.Get("k1", &value));
    ASSERT_EQ(value, std::string("v3"));
    ASSERT_STATUS_CODE(db.Get("k2", &value), Status::kNotFound);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

/// @brief Scan/Fold/Iterator 测试：验证有序遍历、前缀扫描和 fold 聚合行为。
void TestScanFoldAndIterator() {
    const std::string path = "./testdata/test_scan_fold_iter";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());
    ASSERT_STATUS_OK(db.Put("a:1", "v1"));
    ASSERT_STATUS_OK(db.Put("a:2", "v22"));
    ASSERT_STATUS_OK(db.Put("b:1", "v333"));

    std::vector<std::pair<std::string, std::string>> scan_result;
    ASSERT_STATUS_OK(db.Scan("a:", 0, &scan_result));
    ASSERT_EQ(scan_result.size(), static_cast<std::size_t>(2));
    ASSERT_EQ(scan_result[0].first, std::string("a:1"));
    ASSERT_EQ(scan_result[1].first, std::string("a:2"));

    int value_total_len = 0;
    ASSERT_STATUS_OK(db.Fold([&value_total_len](const std::string&, const std::string& value) {
        value_total_len += static_cast<int>(value.size());
        return Status::OK();
    }));
    ASSERT_EQ(value_total_len, 9);

    std::unique_ptr<KVStore::Iterator> iter;
    ASSERT_STATUS_OK(db.NewIterator(&iter));
    ASSERT_TRUE(iter != nullptr);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->Key(), std::string("a:1"));
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->Key(), std::string("a:2"));
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->Key(), std::string("b:1"));
    iter->Next();
    ASSERT_TRUE(!iter->Valid());

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

/// @brief 重启恢复测试：写入数据、关闭后重新打开，验证索引从 segment 或 snapshot 正确恢复。
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

/// @brief 覆盖写恢复测试：写入三个版本关闭后重启，验证恢复到最新版本 v3。
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

/// @brief 删除后恢复测试：Put 两个 key、Delete 其中一个，关闭重启后验证被删 key 不可见。
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

/// @brief 空 key 测试：Put 空字符串 key 应返回 kInvalidArgument，拒绝写入。
void TestEmptyKey() {
    const std::string path = "./testdata/test_empty_key_crc";
    CleanDir(path);

    KVStore db(MakeOptions(path));
    ASSERT_STATUS_OK(db.Open());

    ASSERT_STATUS_CODE(db.Put("", "value"), Status::kInvalidArgument);

    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

/// @brief 查询不存在 key 的测试：Get 一个从未写入的 key，期望返回 kNotFound。
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
/// @brief 大 value 恢复测试：写入 4096 字节的 value，关闭重启后验证完整读回。
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
/// @brief bad magic 检测测试：翻转文件首字节破坏 magic，Open 后期望返回 kCorruption。
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
/// @brief 活跃 segment 尾部截断恢复测试：截掉最后 3 字节模拟崩溃，
///        验证 Open 成功且前面的完整记录仍然可读。
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

/// @brief 多 segment rotate 及恢复测试：设置极小的 max_data_file_size 触发 rotate，
///        验证多个 segment 写入后重启能正确恢复所有 key。
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

/// @brief 旧 segment 尾部截断后继续恢复测试：
///        人为截断第一个 segment 的末尾，验证恢复时跳过损坏部分并继续处理后续 segment。
void TestRecoverContinuesAfterPartialTailInOldSegment() {
    const std::string path = "./testdata/test_recover_partial_old_segment";
    CleanDir(path);

    Options opt = MakeOptions(path);
    opt.max_data_file_size = 70;

    {
        KVStore db(opt);
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("k1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        ASSERT_STATUS_OK(db.Put("k2", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        ASSERT_STATUS_OK(db.Put("k3", "cccccccccccccccccccccccccccccccc"));
        ASSERT_STATUS_OK(db.Close());
    }

    ASSERT_TRUE(fs::exists(DataFilePath(path, 1)));
    ASSERT_TRUE(fs::exists(DataFilePath(path, 2)));
    ASSERT_TRUE(fs::exists(DataFilePath(path, 3)));

    // 人为制造“旧 segment 尾部半条记录”。
    ASSERT_TRUE(TruncateFileTail(DataFilePath(path, 1), 5));

    {
        KVStore db(opt);
        ASSERT_STATUS_OK(db.Open());

        std::string value;
        // 第一段最后一条可能被裁掉，因此 k1 可能丢失。
        Status s1 = db.Get("k1", &value);
        ASSERT_TRUE(s1.ok() || s1.code() == Status::kNotFound);

        // 关键：后续 segment 仍应继续恢复。
        ASSERT_STATUS_OK(db.Get("k2", &value));
        ASSERT_EQ(value, std::string("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        ASSERT_STATUS_OK(db.Get("k3", &value));
        ASSERT_EQ(value, std::string("cccccccccccccccccccccccccccccccc"));

        ASSERT_STATUS_OK(db.Close());
    }

    PassTest(__FUNCTION__);
}

/// @brief 打开不存在路径的测试：以只读模式打开不存在的文件，期望返回 kIOError。
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

/// @brief 越界读取测试：在有效记录之外的偏移调用 Read，期望返回 kOutOfRange。
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

/// @brief CRC 校验失败测试：翻转 value 区域一个字节，Open 时期望返回 kChecksumFailed。
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

/// @brief 非法记录类型测试：翻转 type 字段字节，Open 时期望返回 kCorruption。
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

/// @brief Merge 保留存活 key 测试：写入多版本和 tombstone，Merge 后验证只剩最新存活 key，
///        且 Close + 重启后状态一致。
void TestMergeCompactionKeepsOnlyLiveKeys() {
    const std::string path = "./testdata/test_merge_compaction_live_keys";
    CleanDir(path);

    Options opt = MakeOptions(path);
    opt.max_data_file_size = 80;

    {
        KVStore db(opt);
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("k1", "v1"));
        ASSERT_STATUS_OK(db.Put("k1", "v2"));
        ASSERT_STATUS_OK(db.Put("k2", "v3"));
        ASSERT_STATUS_OK(db.Delete("k2"));
        ASSERT_STATUS_OK(db.Put("k3", "v4"));
        ASSERT_STATUS_OK(db.Put("k4", "v5"));

        ASSERT_TRUE(CountDataFiles(path) >= 2);

        ASSERT_STATUS_OK(db.Merge());
        ASSERT_EQ(CountDataFiles(path), 1);

        std::string value;
        ASSERT_STATUS_OK(db.Get("k1", &value));
        ASSERT_EQ(value, std::string("v2"));
        ASSERT_STATUS_CODE(db.Get("k2", &value), Status::kNotFound);
        ASSERT_STATUS_OK(db.Get("k3", &value));
        ASSERT_EQ(value, std::string("v4"));
        ASSERT_STATUS_OK(db.Get("k4", &value));
        ASSERT_EQ(value, std::string("v5"));
        ASSERT_STATUS_OK(db.Close());
    }

    {
        KVStore db(opt);
        ASSERT_STATUS_OK(db.Open());
        ASSERT_EQ(CountDataFiles(path), 1);

        std::string value;
        ASSERT_STATUS_OK(db.Get("k1", &value));
        ASSERT_EQ(value, std::string("v2"));
        ASSERT_STATUS_CODE(db.Get("k2", &value), Status::kNotFound);
        ASSERT_STATUS_OK(db.Get("k3", &value));
        ASSERT_EQ(value, std::string("v4"));
        ASSERT_STATUS_OK(db.Get("k4", &value));
        ASSERT_EQ(value, std::string("v5"));
        ASSERT_STATUS_OK(db.Close());
    }

    PassTest(__FUNCTION__);
}

/// @brief 空库 Merge 测试：对没有任何写入的数据库调用 Merge，期望不报错且 segment 数量为 1。
void TestMergeOnEmptyDatabase() {
    const std::string path = "./testdata/test_merge_empty_db";
    CleanDir(path);

    Options opt = MakeOptions(path);
    KVStore db(opt);
    ASSERT_STATUS_OK(db.Open());
    ASSERT_STATUS_OK(db.Merge());
    ASSERT_EQ(CountDataFiles(path), 1);
    ASSERT_STATUS_OK(db.Close());

    PassTest(__FUNCTION__);
}

/// @brief Merge 生成 hint 文件测试：Merge 后验证磁盘上存在对应的 .hint 文件。
void TestMergeCreatesHintFile() {
    const std::string path = "./testdata/test_merge_creates_hint";
    CleanDir(path);

    Options opt = MakeOptions(path);
    opt.max_data_file_size = 80;

    KVStore db(opt);
    ASSERT_STATUS_OK(db.Open());
    ASSERT_STATUS_OK(db.Put("k1", "v1"));
    ASSERT_STATUS_OK(db.Put("k2", "v2"));
    ASSERT_STATUS_OK(db.Merge());
    ASSERT_TRUE(CountDataFiles(path) == 1);
    ASSERT_TRUE(fs::exists(HintFilePath(path, 2)));
    ASSERT_STATUS_OK(db.Close());

    PassTest(__FUNCTION__);
}

/// @brief Close 生成 index snapshot 测试：正常写入并 Close 后，验证 index.snapshot 文件存在。
void TestCloseCreatesIndexSnapshot() {
    const std::string path = "./testdata/test_close_creates_snapshot";
    CleanDir(path);

    {
        KVStore db(MakeOptions(path));
        ASSERT_STATUS_OK(db.Open());
        ASSERT_STATUS_OK(db.Put("k1", "v1"));
        ASSERT_STATUS_OK(db.Close());
    }

    ASSERT_TRUE(fs::exists(SnapshotPath(path)));
    PassTest(__FUNCTION__);
}

/// @brief 后台线程自动 Merge 测试：开启后台 merge 后不显式调用 Merge，等待后应自动压缩为单文件。
void TestBackgroundMerge() {
    const std::string path = "./testdata/test_background_merge";
    CleanDir(path);

    Options opt = MakeOptions(path);
    opt.max_data_file_size = 80;
    opt.enable_background_merge = true;
    opt.background_merge_interval_ms = 100;
    opt.background_merge_min_segments = 2;

    KVStore db(opt);
    ASSERT_STATUS_OK(db.Open());
    ASSERT_STATUS_OK(db.Put("k1", "v1"));
    ASSERT_STATUS_OK(db.Put("k2", "v2"));
    ASSERT_STATUS_OK(db.Put("k3", "v3"));
    ASSERT_TRUE(CountDataFiles(path) >= 2);

    std::this_thread::sleep_for(std::chrono::milliseconds(350));
    ASSERT_EQ(CountDataFiles(path), 1);
    ASSERT_STATUS_OK(db.Close());
    PassTest(__FUNCTION__);
}

/// @brief 测试入口：按顺序运行所有测试函数，最终汇总通过/失败数量。
/// 若有任何失败则以非零退出码退出，便于 CI 检测。
int main() {
    TestPutAndGet();
    TestOverwrite();
    TestDelete();
    TestDeleteNonExistentKey();
    TestWriteBatch();
    TestScanFoldAndIterator();
    TestRecoveryAfterReopen();
    TestRecoveryWithOverwrite();
    TestRecoveryWithDelete();
    TestEmptyKey();
    TestGetNonExistentKey();

    TestRecoveryWithLargeValue();
    TestCRCDetectsCorruptionOnOpen();
    TestRecoveryStopsAtPartialTailRecord();
    TestMultiSegmentRotationAndRecovery();
    TestRecoverContinuesAfterPartialTailInOldSegment();
    TestOpenFailureReturnsIOError();
    TestReadOutOfRangeReturnsOutOfRange();
    TestChecksumFailureReturnsChecksumFailed();
    TestInvalidRecordTypeReturnsCorruption();
    TestMergeCompactionKeepsOnlyLiveKeys();
    TestMergeOnEmptyDatabase();
    TestMergeCreatesHintFile();
    TestCloseCreatesIndexSnapshot();
    TestBackgroundMerge();

    std::cout << "\n========== TEST SUMMARY ==========" << std::endl;
    std::cout << "PASSED: " << g_passed << std::endl;
    std::cout << "FAILED: " << g_failed << std::endl;

    if (g_failed != 0) {
        return 1;
    }
    return 0;
}
