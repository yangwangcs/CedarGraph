#pragma once

#include <memory>
#include <string>
#include <vector>
#include <cstdint>

namespace cedar {

// 前向声明
class CedarGraphStorage;
class Timestamp;
enum class AllenRelation;

// 临时占位实现 - TemporalQueryEngine
class TemporalQueryEngine {
public:
    explicit TemporalQueryEngine(CedarGraphStorage* storage) : storage_(storage) {}
    ~TemporalQueryEngine() = default;
    
    void BuildIndex() {}
    
    struct Stats {
        uint64_t index_size = 0;
        uint64_t query_count = 0;
        uint64_t index_entries = 0;
        uint64_t blocks_pruned = 0;
        uint64_t blocks_checked = 0;
        double avg_query_time_ms = 0.0;
    };
    
    Stats GetStats() const { return {}; }
    
private:
    CedarGraphStorage* storage_;
};

} // namespace cedar
