#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_file.h"
#include "index_entry.h"
#include "log_record.h"
#include "options.h"
#include "status.h"

class KVStore {
public:
    struct WriteBatchOp {
        RecordType type = RecordType::kPut;
        std::string key;
        std::string value;
    };

    enum class ReadConsistency {
        kReadCommitted,
        kLinearizable,
    };

    class Iterator {
    public:
        explicit Iterator(std::vector<std::pair<std::string, std::string>> items);
        bool Valid() const;
        void Next();
        const std::string& Key() const;
        const std::string& Value() const;

    private:
        std::vector<std::pair<std::string, std::string>> items_;
        std::size_t index_ = 0;
    };

    explicit KVStore(Options options);
    ~KVStore();

    Status Open();
    Status Close();

    Status Put(const std::string& key, const std::string& value);
    Status Get(const std::string& key, std::string* value);
    Status GetWithConsistency(const std::string& key,
                              std::string* value,
                              ReadConsistency consistency);
    Status Delete(const std::string& key);

    Status WriteBatch(const std::vector<WriteBatchOp>& ops);

    Status NewIterator(std::unique_ptr<Iterator>* iter);
    Status Scan(const std::string& prefix,
                std::size_t limit,
                std::vector<std::pair<std::string, std::string>>* result);
    Status Fold(const std::function<Status(const std::string&, const std::string&)>& fn);

    Status Merge();

private:
    struct WriteRequest {
        std::vector<WriteBatchOp> ops;
        std::promise<Status> done;
        uint64_t sequence = 0;
    };

    void StartBackgroundWorker();
    void StopBackgroundWorker();
    void BackgroundWorkerLoop();

    void StartWriteWorker();
    void StopWriteWorker();
    void WriteWorkerLoop();
    Status ProcessWriteRequest(const WriteRequest& request);
    Status EnqueueWriteRequest(std::vector<WriteBatchOp> ops);
    void WaitForAppliedSequence(uint64_t sequence);

    Status MergeUnlocked();

    Status Recover();

    Status AppendRecord(const LogRecord& record, IndexEntry* entry, bool sync_immediately);

    uint32_t EstimateRecordSize(const LogRecord& record) const;

    Status RotateIfNeeded(uint32_t incoming_record_size);

    Status RotateActiveFile();

    std::string BuildDataFilePath(uint32_t file_id) const;

    std::string BuildIndexSnapshotPath() const;

    std::string BuildHintFilePath(uint32_t file_id) const;

    bool TryLoadIndexSnapshot();

    Status SaveIndexSnapshot() const;

    Status RecoverFromHintFile(uint32_t file_id);

    Status ReadValueByEntry(const IndexEntry& entry, std::string* value);

private:
    Options options_;

    DataFile* active_file_ = nullptr;

    std::unordered_map<uint32_t, std::shared_ptr<DataFile>> data_files_;

    std::vector<uint32_t> ordered_file_ids_;

    std::unordered_map<std::string, IndexEntry> index_;

    uint32_t active_file_id_ = 1;

    std::atomic<bool> opened_{false};

    mutable std::shared_mutex store_mutex_;
    mutable std::shared_mutex index_mutex_;
    mutable std::shared_mutex files_mutex_;
    mutable std::mutex append_mutex_;
    mutable std::mutex state_mutex_;

    std::thread background_worker_;
    std::mutex background_mutex_;
    std::condition_variable background_cv_;
    bool stop_background_worker_ = false;

    std::thread write_worker_;
    std::mutex write_queue_mutex_;
    std::condition_variable write_queue_cv_;
    bool stop_write_worker_ = false;
    std::deque<WriteRequest> write_queue_;

    std::atomic<uint64_t> enqueued_sequence_{0};
    std::atomic<uint64_t> applied_sequence_{0};
    std::mutex applied_sequence_mutex_;
    std::condition_variable applied_sequence_cv_;
};
