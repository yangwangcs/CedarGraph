/**
 * CedarGraph-DTx 分布式事务使用示例
 * 
 * 展示如何使用五项创新技术：
 * 1. GLTR - 图局部性感知事务路由
 * 2. TW-CD - 时序窗口冲突检测
 * 3. LND-OCC - LSM-Tree 原生分布式 OCC
 * 4. DVC-Val - 分布式版本链验证
 * 5. BBCC - 基于书签的因果一致性
 */

#include <iostream>
#include <vector>
#include <memory>
#include <thread>
#include <chrono>

// DTx 头文件
#include "cedar/dtx/partition.h"
#include "cedar/dtx/temporal_window.h"
#include "cedar/dtx/twcd_engine.h"
#include "cedar/dtx/lsm_native_occ.h"
#include "cedar/dtx/version_chain.h"
#include "cedar/dtx/bookmark_manager.h"

using namespace cedar::dtx;

// ============================================================================
// 示例 1: GLTR - 分区路由
// ============================================================================
void Example_GLTR() {
    std::cout << "\n=== Example 1: GLTR (Graph-Locality-Aware Transaction Routing) ===\n";
    
    // 创建分区管理器 (假设有 4 个分区)
    PartitionManager partition_mgr(4);
    
    // 创建一些 CedarKey，每个 key 都有 part_id
    std::vector<CedarKey> keys;
    keys.push_back(CreateKeyWithPartition(100, 1, 0));  // partition 0
    keys.push_back(CreateKeyWithPartition(101, 1, 0));  // partition 0
    keys.push_back(CreateKeyWithPartition(200, 1, 1));  // partition 1
    
    // 检查是否需要协调
    bool needs_coord = partition_mgr.NeedsCoordination(keys);
    std::cout << "Keys span " << partition_mgr.GetPartitionsForKeys(keys).size() 
              << " partitions, needs coordination: " << (needs_coord ? "yes" : "no") << "\n";
    
    // 检查本地事务
    bool is_local = partition_mgr.IsLocalTransaction(0, {keys[0], keys[1]});
    std::cout << "Transaction on partition 0 with keys[0,1] is local: " 
              << (is_local ? "yes" : "no") << "
";
    
    // 打印分区分布
    auto partitions = partition_mgr.GetPartitionsForKeys(keys);
    std::cout << "Partitions involved: ";
    for (auto pid : partitions) {
        std::cout << pid << " ";
    }
    std::cout << "\n";
}

// ============================================================================
// 示例 2: Temporal Window - 时序窗口管理
// ============================================================================
void Example_TemporalWindow() {
    std::cout << "\n=== Example 2: Temporal Window Management ===\n";
    
    // 创建时序窗口 [2024-01-01, 2024-12-31]
    TemporalWindow window(
        1704067200000000ULL,  // 2024-01-01 00:00:00 UTC
        1735689600000000ULL,  // 2024-12-31 00:00:00 UTC
        cedar::TemporalWindowType::SNAPSHOT
    );
    
    std::cout << "Window: [" << window.start_time().value() 
              << ", " << window.end_time().value() << "]\n";
    std::cout << "Duration: " << window.DurationMicros() << " microseconds\n";
    
    // 检查时间戳是否在窗口内
    Timestamp test_time(1710000000000000ULL);  // 2024-03-09
    std::cout << "Test time " << test_time.value() 
              << " in window: " << (window.Contains(test_time) ? "yes" : "no") << "
";
    
    // 窗口重叠检查
    TemporalWindow window2(1720000000000000ULL, 1740000000000000ULL);  // 与 window 部分重叠
    std::cout << "Window overlap: " << (window.Overlaps(window2) ? "yes" : "no") << "
";
}

// ============================================================================
// 示例 3: TW-CD - 时序窗口冲突检测
// ============================================================================
void Example_TWCD() {
    std::cout << "\n=== Example 3: TW-CD (Temporal-Window Conflict Detection) ===\n";
    
    TwcdEngine engine;
    
    // 事务 T1: 读取 2024 年数据
    TemporalWindow window1(1704067200000000ULL, 1735689600000000ULL);
    std::vector<CedarKey> read_set1 = {CreateKeyWithPartition(100, 1, 0)};
    
    auto result1 = engine.RegisterTransaction(1, window1, read_set1, {});
    std::cout << "T1 registered: " << (result1.IsValid() ? "success" : "conflict") << "
";
    
    // 事务 T2: 写入 2025 年数据 (时序不重叠，无冲突)
    TemporalWindow window2(1735689600000001ULL, 1767225600000000ULL);
    std::vector<CedarKey> write_set2 = {CreateKeyWithPartition(100, 1, 0)};  // 相同 key!
    
    auto result2 = engine.CheckConflict(2, window2, {}, write_set2);
    std::cout << "T2 check (different time range, same key): " 
              << (result2.IsValid() ? "NO CONFLICT (TW-CD advantage!)" : "CONFLICT") << "
";
    
    // 事务 T3: 写入 2024 年中 (时序重叠，有冲突)
    TemporalWindow window3(1710000000000000ULL, 1720000000000000ULL);
    auto result3 = engine.CheckConflict(3, window3, {}, write_set2);
    std::cout << "T3 check (overlapping time range): " 
              << (result3.IsValid() ? "NO CONFLICT" : "CONFLICT (expected)") << "
";
    
    // 完成事务
    engine.CompleteTransaction(1);
    engine.CompleteTransaction(2);
}

// ============================================================================
// 示例 4: LND-OCC - 三层提交
// ============================================================================
void Example_LND_OCC() {
    std::cout << "\n=== Example 4: LND-OCC (3-Layer Commit Strategy) ===\n";
    
    LndOccEngine engine;
    
    // 场景 1: 单层分区事务 (Layer 1 - 零协调)
    {
        DistributedTxnContext ctx;
        ctx.txn_id = 100;
        ctx.read_set = {CreateKeyWithPartition(100, 1, 0), 
                        CreateKeyWithPartition(101, 1, 0)};  // 都在 partition 0
        ctx.write_set = {CreateKeyWithPartition(102, 1, 0)};
        
        auto txn_type = engine.ClassifyTransaction(&ctx);
        std::cout << "Single-partition transaction classified as: "
                  << (txn_type == TxnType::kSinglePartition ? "Layer 1 (zero overhead!)" : "other") << "
";
    }
    
    // 场景 2: 跨分区但同时序范围 (Layer 2 - 轻量验证)
    {
        DistributedTxnContext ctx;
        ctx.txn_id = 101;
        ctx.read_set = {CreateKeyWithPartition(100, 1, 0), 
                        CreateKeyWithPartition(200, 1, 1)};  // 跨分区
        ctx.write_set = {CreateKeyWithPartition(300, 1, 2)};
        ctx.temporal_range = std::make_pair(
            Timestamp(1704067200000000ULL),
            Timestamp(1735689600000000ULL)
        );
        
        auto txn_type = engine.ClassifyTransaction(&ctx);
        std::cout << "Cross-partition, same temporal range classified as: "
                  << (txn_type == TxnType::kSameTemporalRange ? "Layer 2 (lightweight)" : "other") << "
";
    }
    
    // 场景 3: 跨分区跨时序范围 (Layer 3 - 完整 2PC)
    {
        DistributedTxnContext ctx;
        ctx.txn_id = 102;
        ctx.read_set = {CreateKeyWithPartition(100, 1, 0)};
        ctx.write_set = {CreateKeyWithPartition(200, 2, 1)};  // 不同时序
        
        auto txn_type = engine.ClassifyTransaction(&ctx);
        std::cout << "Cross-partition, cross-temporal classified as: "
                  << (txn_type == TxnType::kCrossTemporalRange ? "Layer 3 (full 2PC)" : "other") << "
";
    }
}

// ============================================================================
// 示例 5: DVC-Val - 版本链验证
// ============================================================================
void Example_DVC_Val() {
    std::cout << "\n=== Example 5: DVC-Val (O(1) Version Validation) ===\n";
    
    // 创建版本链索引
    VersionChainIndex index(0);  // partition 0
    
    // 创建版本链头
    CedarKey key = CreateKeyWithPartition(100, 1, 0);
    auto* head = index.GetOrCreateHead(key);
    
    // 模拟提交新版本
    head->UpdateVersion(1, Timestamp(1000));
    std::cout << "Created version chain head for key " << key.entity_id() << "\n";
    std::cout << "Latest version: " << head->latest_version() << "\n";
    
    // O(1) 快速验证
    bool valid1 = head->FastValidate(1, Timestamp(500));  // 读版本 == 最新版本
    std::cout << "Fast validate (read_ver == latest): " << (valid1 ? "VALID" : "NEED_TRAVERSE") << "\n";
    
    bool valid2 = head->FastValidate(0, Timestamp(1500));  // 提交时间戳 > 最新提交时间戳
    std::cout << "Fast validate (commit_ts > latest): " << (valid2 ? "VALID" : "NEED_TRAVERSE") << "
";
    
    bool valid3 = head->FastValidate(0, Timestamp(500));  // 需要遍历链
    std::cout << "Fast validate (need traverse): " << (valid3 ? "VALID" : "NEED_TRAVERSE") << "
";
    
    // 批量验证
    std::vector<std::pair<CedarKey, uint64_t>> read_set = {
        {key, 1},
        {CreateKeyWithPartition(101, 1, 0), 0}
    };
    auto results = index.BatchValidate(read_set, Timestamp(2000));
    std::cout << "Batch validated " << results.size() << " keys\n";
}

// ============================================================================
// 示例 6: BBCC - 因果一致性书签
// ============================================================================
void Example_BBCC() {
    std::cout << "\n=== Example 6: BBCC (Bookmark-Based Causal Consistency) ===\n";
    
    // 创建书签管理器
    BookmarkManager bookmark_mgr(0);  // shard 0
    
    // 获取当前 HLC
    auto hlc1 = bookmark_mgr.GetCurrentHLC();
    std::cout << "Current HLC: " << hlc1.ToString() << "\n";
    
    // 模拟分布式更新
    BookmarkHlc remote_hlc(1704067200000000ULL, 5);
    bookmark_mgr.UpdateHLC(remote_hlc);
    
    auto hlc2 = bookmark_mgr.GetCurrentHLC();
    std::cout << "After remote update HLC: " << hlc2.ToString() << "\n";
    std::cout << "HLC ordering: hlc1 happens-before hlc2: " 
              << (hlc1.HappensBefore(hlc2) ? "yes" : "no") << "
";
    
    // 创建书签
    auto bookmark = bookmark_mgr.CreateBookmark();
    std::cout << "Created bookmark: txn_id=" << bookmark.txn_id 
              << ", hlc=" << bookmark.hlc.ToString() << "\n";
    
    // 设置/获取 watermark
    bookmark_mgr.SetPartitionWatermark(0, 100);
    bookmark_mgr.SetPartitionWatermark(1, 200);
    auto min_watermark = bookmark_mgr.GetGlobalMinWatermark();
    std::cout << "Global min watermark: " << min_watermark << "\n";
    
    // 会话书签
    std::string session_id = "user-session-123";
    bookmark_mgr.UpdateSessionBookmark(session_id, bookmark);
    auto retrieved = bookmark_mgr.GetSessionBookmark(session_id);
    if (retrieved) {
        std::cout << "Retrieved session bookmark: " << retrieved->hlc.ToString() << "\n";
    }
    
    // 因果一致性检查
    CausalConsistencyChecker checker;
    DistributedBookmark newer_bookmark;
    newer_bookmark.hlc = bookmark.hlc;
    newer_bookmark.hlc.Update(BookmarkHlc::Now());
    
    bool ryw = checker.CheckReadYourWrites(bookmark, newer_bookmark);
    std::cout << "Read-Your-Writes check: " << (ryw ? "SATISFIED" : "VIOLATED") << "
";
    
    bool monotonic = checker.CheckMonotonicReads(bookmark, newer_bookmark);
    std::cout << "Monotonic-Reads check: " << (monotonic ? "SATISFIED" : "VIOLATED") << "
";
}

// ============================================================================
// 示例 7: 完整分布式事务流程
// ============================================================================
void Example_FullDistributedTxn() {
    std::cout << "\n=== Example 7: Full Distributed Transaction Flow ===\n";
    
    // 1. 初始化组件
    PartitionManager partition_mgr(4);
    TwcdEngine twcd_engine;
    LndOccEngine lnd_engine;
    VersionChainIndex version_index(0);
    BookmarkManager bookmark_mgr(0);
    
    // 2. 创建事务上下文
    DistributedTxnContext ctx;
    ctx.txn_id = 1000;
    ctx.txn_type = TxnType::kDistributed;
    ctx.start_time = std::chrono::steady_clock::now();
    
    // 3. 定义读写集合 (跨 2 个分区)
    ctx.read_set = {
        CreateKeyWithPartition(100, 1, 0),  // partition 0
        CreateKeyWithPartition(200, 1, 1),  // partition 1
    };
    ctx.write_set = {
        CreateKeyWithPartition(101, 1, 0),  // partition 0
        CreateKeyWithPartition(201, 1, 1),  // partition 1
    };
    ctx.temporal_range = std::make_pair(
        Timestamp(1704067200000000ULL),
        Timestamp(1735689600000000ULL)
    );
    
    // 4. 检查分区需求
    auto partitions = partition_mgr.GetPartitionsForKeys(ctx.AllKeys());
    std::cout << "Transaction spans " << partitions.size() << " partitions\n";
    
    // 5. TW-CD 冲突检测
    auto conflict_result = twcd_engine.CheckConflict(
        ctx.txn_id,
        TemporalWindow(ctx.temporal_range.first, ctx.temporal_range.second),
        ctx.read_set,
        ctx.write_set
    );
    
    if (!conflict_result.IsValid()) {
        std::cout << "CONFLICT DETECTED by TW-CD: " << conflict_result.ToString() << "\n";
        return;
    }
    std::cout << "TW-CD check passed\n";
    
    // 6. 分类并执行提交
    auto txn_type = lnd_engine.ClassifyTransaction(&ctx);
    std::cout << "Transaction classified as: " << TxnTypeToString(txn_type) << "\n";
    
    // 7. 版本验证
    std::vector<std::pair<CedarKey, uint64_t>> validation_set;
    for (const auto& key : ctx.read_set) {
        validation_set.push_back({key, 0});  // 假设读取版本 0
    }
    auto validation_results = version_index.BatchValidate(
        validation_set, 
        Timestamp(1700000000000000ULL)
    );
    std::cout << "Version validation: " << validation_results.size() << " keys checked\n";
    
    // 8. 创建书签
    auto bookmark = bookmark_mgr.CreateBookmark();
    std::cout << "Transaction committed with bookmark: " << bookmark.hlc.ToString() << "\n";
    
    // 9. 清理
    twcd_engine.CompleteTransaction(ctx.txn_id);
    std::cout << "Transaction completed successfully!\n";
}

// ============================================================================
// 主函数
// ============================================================================
int main() {
    std::cout << "╔═══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║        CedarGraph-DTx Distributed Transaction Examples        ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════════╝\n";
    
    Example_GLTR();
    Example_TemporalWindow();
    Example_TWCD();
    Example_LND_OCC();
    Example_DVC_Val();
    Example_BBCC();
    Example_FullDistributedTxn();
    
    std::cout << "\n═══════════════════════════════════════════════════════════════\n";
    std::cout << "All examples completed successfully!\n";
    std::cout << "═══════════════════════════════════════════════════════════════\n";
    
    return 0;
}
