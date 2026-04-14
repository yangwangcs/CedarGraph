/**
 * CedarGraph 事务模块示例代码
 * 
 * 编译:
 *   g++ -std=c++17 -pthread transaction_examples.cc -o transaction_demo
 * 
 * 运行:
 *   ./transaction_demo
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <random>
#include <cassert>
#include <unordered_map>
#include <atomic>

// ============================================================================
// 模拟CedarGraph事务API (实际使用时替换为真实API)
// ============================================================================

namespace cedar {
namespace txn_demo {

using Timestamp = uint64_t;
using TxnId = uint64_t;

// 获取当前时间戳 (微秒)
Timestamp Now() {
    auto now = std::chrono::high_resolution_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
    return static_cast<uint64_t>(us);
}

// ============================================================================
// 版本数据 (MVCC)
// ============================================================================
template<typename T>
struct VersionedValue {
    T value;
    Timestamp ts;
    VersionedValue<T>* next;
    
    VersionedValue(T v, Timestamp t) 
        : value(v), ts(t), next(nullptr) {}
};

// ============================================================================
// OCC事务上下文
// ============================================================================
class TxnContext {
public:
    enum class State { ACTIVE, PREPARING, COMMITTED, ABORTED };
    
    TxnId txn_id;
    Timestamp start_ts;
    State state;
    
    // 读集合: <key, 读取时的版本>
    std::unordered_map<std::string, Timestamp> read_set;
    
    // 写集合: <key, 新值>
    std::unordered_map<std::string, int64_t> write_set;
    
    TxnContext(TxnId id) : txn_id(id), start_ts(Now()), state(State::ACTIVE) {}
    
    void Abort() { state = State::ABORTED; }
    void Commit() { state = State::COMMITTED; }
};

// ============================================================================
// 模拟数据库存储
// ============================================================================
class MockDatabase {
public:
    // 数据存储: 版本链
    std::unordered_map<std::string, VersionedValue<int64_t>*> data_;
    
    // 事务ID生成器
    std::atomic<TxnId> next_txn_id_{1};
    
    // 统计
    std::atomic<uint64_t> committed_{0};
    std::atomic<uint64_t> aborted_{0};
    std::atomic<uint64_t> retries_{0};
    
    ~MockDatabase() {
        for (auto& [key, head] : data_) {
            while (head) {
                auto* next = head->next;
                delete head;
                head = next;
            }
        }
    }
    
    // 开始事务
    TxnContext Begin() {
        return TxnContext(next_txn_id_.fetch_add(1));
    }
    
    // MVCC读取
    int64_t Read(TxnContext& txn, const std::string& key) {
        if (txn.state != TxnContext::State::ACTIVE) {
            throw std::runtime_error("Transaction not active");
        }
        
        // 检查写集合 (读己之写)
        auto it = txn.write_set.find(key);
        if (it != txn.write_set.end()) {
            return it->second;
        }
        
        // 从版本链读取
        auto dit = data_.find(key);
        if (dit == data_.end()) {
            throw std::runtime_error("Key not found: " + key);
        }
        
        VersionedValue<int64_t>* version = dit->second;
        
        // 找小于等于start_ts的最新版本
        VersionedValue<int64_t>* target = nullptr;
        while (version) {
            if (version->ts <= txn.start_ts) {
                target = version;
                break;
            }
            version = version->next;
        }
        
        if (!target) {
            throw std::runtime_error("No visible version for key: " + key);
        }
        
        // 记录读集合
        txn.read_set[key] = target->ts;
        
        return target->value;
    }
    
    // 写入 (仅记录到写集合)
    void Write(TxnContext& txn, const std::string& key, int64_t value) {
        if (txn.state != TxnContext::State::ACTIVE) {
            throw std::runtime_error("Transaction not active");
        }
        txn.write_set[key] = value;
    }
    
    // OCC验证并提交
    bool Commit(TxnContext& txn) {
        if (txn.state != TxnContext::State::ACTIVE) {
            return false;
        }
        
        txn.state = TxnContext::State::PREPARING;
        
        // 验证阶段: 检查read_set中的数据是否被修改
        for (const auto& [key, read_ts] : txn.read_set) {
            auto dit = data_.find(key);
            if (dit == data_.end()) continue;
            
            // 检查是否有新版本在start_ts之后提交
            VersionedValue<int64_t>* version = dit->second;
            if (version->ts > read_ts && version->ts > txn.start_ts) {
                // 冲突! 数据在我们读取后被修改
                txn.Abort();
                aborted_.fetch_add(1);
                return false;
            }
        }
        
        // 写阶段: 应用所有写入
        Timestamp commit_ts = Now();
        for (const auto& [key, value] : txn.write_set) {
            auto* new_version = new VersionedValue<int64_t>(value, commit_ts);
            
            auto dit = data_.find(key);
            if (dit != data_.end()) {
                // 插入版本链头部
                new_version->next = dit->second;
                dit->second = new_version;
            } else {
                data_[key] = new_version;
            }
        }
        
        txn.Commit();
        committed_.fetch_add(1);
        return true;
    }
};

// ============================================================================
// 示例1: 基础转账事务
// ============================================================================
void Example1_BasicTransfer() {
    std::cout << "\n========== 示例1: 基础转账事务 ==========\n";
    
    MockDatabase db;
    
    // 初始化账户
    {
        auto txn = db.Begin();
        db.Write(txn, "Alice", 1000);
        db.Write(txn, "Bob", 500);
        db.Commit(txn);
    }
    
    std::cout << "初始状态:\n";
    {
        auto txn = db.Begin();
        std::cout << "  Alice: " << db.Read(txn, "Alice") << "\n";
        std::cout << "  Bob: " << db.Read(txn, "Bob") << "\n";
    }
    
    // 执行转账
    {
        auto txn = db.Begin();
        
        int64_t alice_balance = db.Read(txn, "Alice");
        int64_t bob_balance = db.Read(txn, "Bob");
        int64_t amount = 100;
        
        if (alice_balance >= amount) {
            db.Write(txn, "Alice", alice_balance - amount);
            db.Write(txn, "Bob", bob_balance + amount);
            
            if (db.Commit(txn)) {
                std::cout << "转账成功: Alice -> Bob " << amount << "\n";
            } else {
                std::cout << "转账失败 (冲突)\n";
            }
        } else {
            txn.Abort();
            std::cout << "转账失败 (余额不足)\n";
        }
    }
    
    std::cout << "最终状态:\n";
    {
        auto txn = db.Begin();
        std::cout << "  Alice: " << db.Read(txn, "Alice") << "\n";
        std::cout << "  Bob: " << db.Read(txn, "Bob") << "\n";
    }
}

// ============================================================================
// 示例2: MVCC快照读取
// ============================================================================
void Example2_MVCCSnapshot() {
    std::cout << "\n========== 示例2: MVCC快照读取 ==========\n";
    
    MockDatabase db;
    
    // T1: 初始化
    TxnId t1_id;
    {
        auto txn = db.Begin();
        t1_id = txn.txn_id;
        db.Write(txn, "X", 100);
        db.Commit(txn);
        std::cout << "T1 (init): X = 100, timestamp = " << txn.start_ts << "\n";
    }
    
    // T2: 开始事务 (创建快照)
    TxnId t2_start_ts;
    {
        auto txn = db.Begin();
        t2_start_ts = txn.start_ts;
        int64_t x1 = db.Read(txn, "X");
        std::cout << "T2 (start): X = " << x1 << " (快照)\n";
        
        // T3: 并发修改
        {
            auto txn3 = db.Begin();
            db.Write(txn3, "X", 200);
            db.Commit(txn3);
            std::cout << "T3 (commit): X = 200\n";
        }
        
        // T2: 仍然读到快照值
        int64_t x2 = db.Read(txn, "X");
        std::cout << "T2 (after T3 commit): X = " << x2 
                  << " (仍然是快照值!)\n";
        
        txn.Abort();  // 只读事务
    }
    
    // T4: 新事务读取最新值
    {
        auto txn = db.Begin();
        int64_t x = db.Read(txn, "X");
        std::cout << "T4 (new): X = " << x << " (最新值)\n";
        txn.Abort();
    }
}

// ============================================================================
// 示例3: OCC冲突检测与重试
// ============================================================================
void Example3_OCCConflict() {
    std::cout << "\n========== 示例3: OCC冲突检测 ==========\n";
    
    MockDatabase db;
    
    // 初始化
    {
        auto txn = db.Begin();
        db.Write(txn, "Counter", 0);
        db.Commit(txn);
    }
    
    std::cout << "初始: Counter = 0\n";
    std::cout << "两个事务并发增加Counter...\n";
    
    // 模拟两个并发事务
    std::thread t1([&db]() {
        auto txn = db.Begin();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        int64_t val = db.Read(txn, "Counter");
        std::cout << "T1 读取: Counter = " << val << "\n";
        
        db.Write(txn, "Counter", val + 10);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        
        if (db.Commit(txn)) {
            std::cout << "T1 提交成功: Counter = " << (val + 10) << "\n";
        } else {
            std::cout << "T1 冲突回滚\n";
        }
    });
    
    std::thread t2([&db]() {
        auto txn = db.Begin();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        
        int64_t val = db.Read(txn, "Counter");
        std::cout << "T2 读取: Counter = " << val << "\n";
        
        db.Write(txn, "Counter", val + 20);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        if (db.Commit(txn)) {
            std::cout << "T2 提交成功: Counter = " << (val + 20) << "\n";
        } else {
            std::cout << "T2 冲突回滚\n";
        }
    });
    
    t1.join();
    t2.join();
    
    // 最终结果
    {
        auto txn = db.Begin();
        int64_t final_val = db.Read(txn, "Counter");
        std::cout << "最终: Counter = " << final_val << "\n";
        txn.Abort();
    }
    
    std::cout << "统计: 提交=" << db.committed_ 
              << ", 回滚=" << db.aborted_ << "\n";
}

// ============================================================================
// 示例4: TWCD时间窗口冲突检测
// ============================================================================
struct Booking {
    std::string user_id;
    Timestamp start_time;
    Timestamp end_time;
};

class RoomBookingSystem {
public:
    std::vector<Booking> bookings_;
    
    // 检查时间冲突
    bool CheckConflict(Timestamp start, Timestamp end) {
        for (const auto& b : bookings_) {
            // 时间窗口重叠检测
            // 重叠条件: !(b.end <= start || b.start >= end)
            // 即: b.start < end && b.end > start
            if (b.start_time < end && b.end_time > start) {
                return true;  // 冲突
            }
        }
        return false;  // 无冲突
    }
    
    bool Book(const std::string& user, Timestamp start, Timestamp end) {
        if (CheckConflict(start, end)) {
            return false;
        }
        bookings_.push_back({user, start, end});
        return true;
    }
};

void Example4_TWCD() {
    std::cout << "\n========== 示例4: TWCD时间窗口冲突 ==========\n";
    
    RoomBookingSystem system;
    
    // 时间轴: 0-10小时
    // 预订1: 9:00-11:00
    bool r1 = system.Book("Alice", 9, 11);
    std::cout << "Alice 预订 9:00-11:00: " << (r1 ? "成功" : "失败") << "\n";
    
    // 预订2: 11:00-13:00 (不冲突)
    bool r2 = system.Book("Bob", 11, 13);
    std::cout << "Bob 预订 11:00-13:00: " << (r2 ? "成功" : "失败") << "\n";
    
    // 预订3: 10:00-12:00 (冲突! 与Alice重叠)
    bool r3 = system.Book("Charlie", 10, 12);
    std::cout << "Charlie 预订 10:00-12:00: " << (r3 ? "成功" : "失败") << "\n";
    
    // 预订4: 13:00-15:00 (不冲突)
    bool r4 = system.Book("David", 13, 15);
    std::cout << "David 预订 13:00-15:00: " << (r4 ? "成功" : "失败") << "\n";
    
    std::cout << "\n当前预订列表:\n";
    for (const auto& b : system.bookings_) {
        std::cout << "  " << b.user_id << ": " 
                  << b.start_time << ":00-" << b.end_time << ":00\n";
    }
}

// ============================================================================
// 示例5: 批量事务
// ============================================================================
void Example5_BatchTransaction() {
    std::cout << "\n========== 示例5: 批量事务 ==========\n";
    
    MockDatabase db;
    
    // 批量插入1000条记录
    auto start = std::chrono::high_resolution_clock::now();
    
    {
        auto txn = db.Begin();
        for (int i = 0; i < 1000; ++i) {
            std::string key = "User_" + std::to_string(i);
            db.Write(txn, key, i * 10);
        }
        db.Commit(txn);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    std::cout << "批量插入1000条记录: " << duration.count() << " μs\n";
    std::cout << "平均每条: " << (duration.count() / 1000.0) << " μs\n";
    
    // 验证
    {
        auto txn = db.Begin();
        std::cout << "验证: User_500 = " << db.Read(txn, "User_500") << "\n";
    }
}

// ============================================================================
// 示例6: 事务重试机制
// ============================================================================
bool TransferWithRetry(MockDatabase& db, 
                       const std::string& from,
                       const std::string& to,
                       int64_t amount,
                       int max_retries = 10) {
    for (int attempt = 0; attempt < max_retries; ++attempt) {
        auto txn = db.Begin();
        
        try {
            int64_t from_balance = db.Read(txn, from);
            if (from_balance < amount) {
                std::cout << "  余额不足\n";
                return false;
            }
            
            int64_t to_balance = db.Read(txn, to);
            
            db.Write(txn, from, from_balance - amount);
            db.Write(txn, to, to_balance + amount);
            
            if (db.Commit(txn)) {
                return true;  // 成功
            }
            
            // 冲突, 重试
            db.retries_.fetch_add(1);
            std::cout << "  冲突, 重试 #" << (attempt + 1) << "\n";
            
            // 指数退避
            std::this_thread::sleep_for(
                std::chrono::milliseconds(1 << attempt));
                
        } catch (...) {
            txn.Abort();
            throw;
        }
    }
    
    return false;  // 超过重试次数
}

void Example6_RetryMechanism() {
    std::cout << "\n========== 示例6: 事务重试机制 ==========\n";
    
    MockDatabase db;
    
    // 初始化
    {
        auto txn = db.Begin();
        db.Write(txn, "Account_A", 10000);
        db.Write(txn, "Account_B", 10000);
        db.Commit(txn);
    }
    
    // 并发转账测试
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&db, &success_count, i]() {
            std::cout << "Thread " << i << " 开始转账\n";
            bool success = TransferWithRetry(db, "Account_A", "Account_B", 100);
            if (success) {
                success_count.fetch_add(1);
                std::cout << "Thread " << i << " 转账成功\n";
            } else {
                std::cout << "Thread " << i << " 转账失败\n";
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    std::cout << "\n结果:\n";
    std::cout << "  成功转账: " << success_count << "/10\n";
    std::cout << "  总重试次数: " << db.retries_ << "\n";
    
    {
        auto txn = db.Begin();
        std::cout << "  Account_A: " << db.Read(txn, "Account_A") << "\n";
        std::cout << "  Account_B: " << db.Read(txn, "Account_B") << "\n";
    }
}

} // namespace txn_demo
} // namespace cedar

// ============================================================================
// 主函数
// ============================================================================
int main() {
    using namespace cedar::txn_demo;
    
    std::cout << "========================================\n";
    std::cout << "CedarGraph 事务模块示例\n";
    std::cout << "========================================\n";
    
    try {
        Example1_BasicTransfer();
        Example2_MVCCSnapshot();
        Example3_OCCConflict();
        Example4_TWCD();
        Example5_BatchTransaction();
        Example6_RetryMechanism();
        
        std::cout << "\n========================================\n";
        std::cout << "所有示例执行完成!\n";
        std::cout << "========================================\n";
        
    } catch (const std::exception& e) {
        std::cerr << "错误: " << e.what() << "\n";
        return 1;
    }
    
    return 0;
}
