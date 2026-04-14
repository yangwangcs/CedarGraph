# CedarGraph 分布式图数据库 - 5分钟快速上手

## 1. 一句话介绍

CedarGraph 是一个支持分布式事务的图数据库，提供强一致性保证和高性能读写。

## 2. 核心概念 (3分钟理解)

### 2.1 图数据库是什么?

```
普通关系型数据库: 表格存储
  Users 表: id | name | age
  Orders 表: id | user_id | amount
  查询需要 JOIN

图数据库: 节点 + 边
  节点(Vertex): 用户、商品、订单
  边(Edge): 用户-购买->商品, 用户-好友->用户
  查询直接遍历图
```

### 2.2 CedarGraph 怎么存?

```
┌─────────────────────────────────────────────────────────────┐
│  数据分片 (Partition by entity_id hash)                      │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐               │
│  │ Node 1    │  │ Node 2    │  │ Node 3    │               │
│  │ User A    │  │ User B    │  │ User C    │               │
│  │ Order #1  │  │ Order #2  │  │ Order #3  │               │
│  └───────────┘  └───────────┘  └───────────┘               │
│                                                             │
│  每个分区使用 Raft 保证强一致                               │
│  跨分区事务使用 2PC 保证原子性                              │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 事务保证?

```
ACID 特性:
- Atomicity (原子性): 要么全成功，要么全失败
- Consistency (一致性): 数据始终有效
- Isolation (隔离性): 事务互不干扰
- Durability (持久性): 提交后不丢失
```

## 3. 代码示例 (2分钟上手)

### 3.1 完整示例

```cpp
#include "cedar/graph/cedar_graph.h"
#include "cedar/driver/session.h"

using namespace cedar;
using namespace cedar::graph;
using namespace cedar::driver;

int main() {
    // 1. 连接数据库
    GraphDb db("localhost:7687");
    auto session = db.NewSession();
    
    // 2. 执行事务
    {
        auto txn = session.BeginTransaction();
        
        // 创建节点
        auto alice = txn.CreateVertex("Person");
        alice.SetProperty("name", "Alice");
        alice.SetProperty("age", 30);
        
        // 创建边
        auto bob = txn.GetVertex("Person", "name", "Bob");
        txn.CreateEdge(alice, bob, "KNOWS");
        
        // 提交
        txn.Commit();
    }
    
    // 3. 查询
    {
        auto txn = session.BeginTransaction();
        auto alice = txn.GetVertex("Person", "name", "Alice");
        
        // 遍历边
        auto friends = alice.GetEdges("KNOWS");
        for (auto& f : friends) {
            std::cout << f.GetProperty("name") << std::endl;
        }
    }
    
    return 0;
}
```

### 3.2 编译运行

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build
cmake .. -DBUILD_EXAMPLES=ON
make cedar_graph_example
./examples/cedar_graph_example
```

## 4. 什么时候用 CedarGraph?

| 场景 | 例子 | CedarGraph优势 |
|------|------|----------------|
| 社交网络 | 好友关系、推荐系统 | 高效遍历关系 |
| 金融交易 | 转账、支付 | 强一致性保证 |
| 知识图谱 | 实体关系、推理 | 灵活 schema |
| 权限管理 | RBAC、ABAC | 快速权限检查 |

## 5. 性能指标

| 操作 | 性能 | 延迟 |
|------|------|------|
| 点查 | 500万+ ops/sec | 0.2μs |
| 写入 | 194万 ops/sec | P99 10μs |
| 事务 | 10万+ txn/sec | P99 5ms |

## 6. 下一步

1. **详细学习**: 阅读 [事务模块指南](./TRANSACTION_MODULE_COMPLETE_GUIDE.md)
2. **动手实践**: 运行 `examples/cedar_graph_example.cc`
3. **性能测试**: 运行 `tests/test_3node_cluster --benchmark`

## 7. 求助

- 问题1: 连接失败? → 检查网络配置
- 问题2: 事务冲突? → 减小事务范围
- 问题3: 性能不足? → 增加节点数量

完整文档: [CedarGraph完全指南](./CEDARGRAPH_COMPLETE_GUIDE.md)
