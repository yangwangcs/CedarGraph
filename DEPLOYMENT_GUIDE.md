# CedarGraph 时间索引系统 - 部署指南

**版本**: v1.0  
**适用场景**: 单机/单机房部署  
**目标**: 30分钟内完成上线

---

## 1. 快速启动（5分钟）

### 1.1 最小配置启动

```cpp
#include "cedar/dtx/storage/raft_storage_integration.h"
#include "cedar/dtx/index/integrated_time_index_system.h"

using namespace cedar::dtx;

int main() {
    // 1. 创建存储服务
    storage::MultiRaftStorageService storage;
    storage::MultiRaftStorageService::Config storage_config;
    storage_config.node_id = 1;
    storage_config.data_root = "/data/cedar";
    storage_config.default_partition_count = 128;
    
    storage.Initialize(storage_config);
    storage.CreateSpace("production", 128, {1});
    
    // 2. 启用时间索引（核心配置）
    index::IntegratedTimeIndexConfig index_config;
    index_config.enable_time_index = true;
    index_config.default_ttl_days = 90;  // 90天自动过期
    index_config.index_data_dir = "/data/cedar/index";
    
    // CDC 配置
    index_config.cdc_config.max_queue_size = 100000;
    index_config.cdc_config.batch_size = 500;
    index_config.cdc_config.consumer_threads = 4;
    
    // 启用
    storage.EnableTimeIndex(index_config);
    
    // 3. 启动完成
    std::cout << "✅ Time Index System Ready!" << std::endl;
    
    // 运行...
    
    return 0;
}
```

### 1.2 配置文件示例

```yaml
# /etc/cedar/time_index.yaml
enable_time_index: true
index_data_dir: /data/cedar/index
default_ttl_days: 90

# CDC 配置
cdc:
  max_queue_size: 100000
  batch_size: 500
  batch_timeout_ms: 50
  consumer_threads: 4
  
# 索引构建配置
index_builder:
  threads_per_partition: 2
  batch_write_size: 1000
  write_buffer_size_mb: 64
  salt_count: 16  # 打散因子

# 查询配置
scatter_gather:
  concurrency: 32
  shard_timeout_ms: 2000
  total_timeout_ms: 10000
  enable_lookup_back: true

# 监控配置（可选）
metrics:
  enable: true
  port: 9091
  export_interval_seconds: 15
```

---

## 2. 生产检查清单

### 2.1 部署前检查

- [ ] 磁盘空间充足（索引约为主表的 30-50%）
- [ ] `/data/cedar` 目录已创建且有写权限
- [ ] 端口 9091 未被占用（如启用监控）
- [ ] 系统时间同步（NTP）

### 2.2 关键配置确认

```bash
# 检查数据目录
ls -la /data/cedar/
# 预期: 存在并有写权限

# 检查磁盘空间
df -h /data
# 预期: 可用空间 > 预估数据量的 2 倍

# 检查端口
lsof -i :9091 || echo "Port 9091 is free"
```

### 2.3 启动验证

```bash
# 1. 编译
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j4

# 2. 运行测试
./examples/time_index_unit_test
# 预期: All tests passed!

# 3. 启动服务
./storaged --config=/etc/cedar/config.yaml

# 4. 验证状态
curl http://localhost:9091/metrics
# 预期: 返回 Prometheus 指标
```

---

## 3. 监控告警配置（可选但推荐）

### 3.1 添加 Webhook 告警

```cpp
// 在 Initialize 之后添加
auto* time_index = storage.GetTimeIndexSystem();

// 配置 Webhook
index::WebhookConfig webhook;
webhook.url = "https://your-monitoring.com/alerts";
webhook.method = "POST";
webhook.headers = {{"Authorization", "Bearer your-token"}};

time_index->AddWebhookChannel(webhook);

// 设置路由规则：Critical 级别发送告警
time_index->SetAlertRoutingRule(
    cedar::dtx::storage::AlertSeverity::kCritical, 
    {"webhook"});
```

### 3.2 关键告警指标

| 指标 | 阈值 | 说明 |
|------|------|------|
| index_lag_ms | > 5000ms | 索引延迟过高 |
| query_latency_p99 | > 1000ms | 查询延迟过高 |
| cdc_queue_size | > 50000 | CDC 队列积压 |
| index_error_rate | > 1% | 索引错误率过高 |

### 3.3 Prometheus 查询示例

```promql
# 索引延迟
cedar_time_index_lag_ms

# 查询 QPS
rate(cedar_time_index_queries_total[1m])

# 错误率
cedar_time_index_errors_total / cedar_time_index_operations_total
```

---

## 4. 常用运维操作

### 4.1 查看系统状态

```cpp
auto* time_index = storage.GetTimeIndexSystem();
auto stats = time_index->GetStats();

std::cout << "=== Time Index Status ===" << std::endl;
std::cout << "Health: " << (stats.is_healthy ? "✅" : "❌") << std::endl;
std::cout << "CDC Published: " << stats.cdc_stats.total_published << std::endl;
std::cout << "Index Entries: " << stats.index_stats.total_entries << std::endl;
std::cout << "Query Total: " << stats.query_stats.total_queries << std::endl;
```

### 4.2 手动触发 Compaction

```cpp
// 对特定分区强制 Compaction
time_index->ForceCompaction(partition_id);
```

### 4.3 调整 TTL

```cpp
// 动态调整 TTL 策略
time_index->SetTtlPolicy(30);  // 30 天
time_index->ExecuteTtlCleanup();  // 立即执行清理
```

### 4.4 禁用时间索引（降级）

```cpp
// 紧急情况下禁用时间索引
storage.DisableTimeIndex();
// 主表写入不受影响
```

---

## 5. 故障排查

### 5.1 索引延迟过高

```bash
# 检查 CDC 队列
# 指标: cdc_queue_size
# 解决: 增加 consumer_threads

# 检查磁盘 I/O
iostat -x 1
# 解决: 使用 SSD 或增加写入缓冲区
```

### 5.2 查询超时

```bash
# 检查并发度配置
# 配置: scatter_gather.concurrency
# 解决: 增加并发度或优化查询范围
```

### 5.3 存储空间不足

```bash
# 检查索引大小
du -sh /data/cedar/index

# 调整 TTL
# 减少 default_ttl_days
# 执行 ExecuteTtlCleanup()
```

---

## 6. 性能基线（参考）

| 指标 | 预期值 | 测试方法 |
|------|--------|----------|
| 索引写入延迟 | < 10ms (P99) | 压测工具 |
| 单分区查询 | < 5ms (P99) | 单元测试 |
| 全局 Scatter 查询 | < 100ms (P99) | 集成测试 |
| CDC 吞吐量 | > 50K events/s | 压测工具 |
| 内存占用 | < 500MB | 系统监控 |

---

## 7. 升级计划

### Phase 1: 立即上线（单机）
- 当前状态: ✅ 就绪
- 操作: 按本指南部署

### Phase 2: 1-2周后（压测 + libcurl）
```bash
# 接入 libcurl 用于告警
# 配置 CMake
find_package(CURL REQUIRED)
target_link_libraries(cedar ${CURL_LIBRARIES})
```

### Phase 3: 1个月后（多机房）
- 实现 gRPC 传输层
- 配置跨机房复制
- 参考: `cross_dc_replication.h`

---

## 8. 联系支持

- **文档**: https://docs.cedargraph.io
- **问题**: https://github.com/cedargraph/issues
- **邮件**: support@cedargraph.io

---

**祝部署顺利！** 🚀
