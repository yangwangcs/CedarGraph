# Raft 实现选型分析

## 候选方案对比

| 特性 | braft (百度) | etcd-raft | 自研 |
|-----|-------------|-----------|------|
| **语言** | C++ | Go | C++ |
| **成熟度** | ⭐⭐⭐ 高 (百度大规模使用) | ⭐⭐⭐ 极高 (etcd 核心) | ⭐ 低 |
| **性能** | ⭐⭐⭐ 极高 | ⭐⭐ 高 (cgo 开销) | 视实现而定 |
| **集成难度** | ⭐⭐ 中等 | ⭐⭐⭐ 需 cgo | ⭐⭐⭐⭐⭐ 极高 |
| **功能丰富度** | ⭐⭐⭐ 完整 (含 Leader 选举、快照、成员变更) | ⭐⭐⭐ 完整 | 需自行实现 |
| **社区活跃** | ⭐⭐ 中文社区活跃 | ⭐⭐⭐ 全球活跃 | - |
| **CedarGraph 适配** | ⭐⭐⭐ 原生 C++ | ⭐⭐ 需适配层 | ⭐⭐⭐ 完全可控 |

## 推荐方案：braft

### 理由

1. **语言匹配**: CedarGraph 是 C++ 项目，braft 原生支持，无需 cgo 桥接
2. **性能**: 百度内部大规模验证，支持 10万+ TPS
3. **功能完整**:
   - Leader 选举与切换
   - 日志复制与快照
   - 成员动态变更 (Add/Remove Node)
   - PreVote 优化 (避免网络分区时频繁选举)
   - Learner 角色 (用于副本扩容)

4. **与现有架构集成**:
```cpp
// CedarGraph 状态机实现
class MetaRaftStateMachine : public braft::StateMachine {
 public:
    // Apply 日志到 MetaD
    void on_apply(braft::Iterator& iter) override {
        for (; iter.valid(); iter.next()) {
            auto entry = iter.done()->data();
            ApplyLogEntry(entry);  // 调用 MetaService
        }
    }
    
    // 生成快照 (用于快速恢复和扩容)
    void on_snapshot_save(braft::SnapshotWriter* writer) override {
        // 序列化 Space/Partition/Node 元数据到 SST
        meta_service_->SaveSnapshot(writer->get_path());
    }
    
    // 加载快照
    int on_snapshot_load(braft::SnapshotReader* reader) override {
        return meta_service_->LoadSnapshot(reader->get_path());
    }
};
```

### 部署架构

```
┌─────────────────────────────────────────┐
│           MetaD Cluster (3/5 nodes)     │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │ Node 1  │ │ Node 2  │ │ Node 3  │   │
│  │ Leader  │ │Follower │ │Follower │   │
│  │ + braft │ │ + braft │ │ + braft │   │
│  └────┬────┘ └────┬────┘ └────┬────┘   │
│       │           │           │         │
│       └───────────┴───────────┘         │
│              Raft Consensus             │
└─────────────────────────────────────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │   MetaService API   │
        │  (Space/Partition)  │
        └─────────────────────┘
```

### 替代方案：etcd-raft (如果 braft 不适应)

**优点**:
- 经过 etcd 生产验证
- 更活跃的社区

**缺点**:
- Go 实现，需要 cgo 或独立进程
- 独立进程方案增加部署复杂度

**结论**: 首选 braft，仅在遇到无法解决的兼容性问题时考虑 etcd-raft + sidecar 模式。

---

## 实施计划

### Phase 1: braft 集成 (2 周)

```cpp
// 新增文件
src/dtx/raft/braft_node.h
src/dtx/raft/braft_node.cc
src/dtx/raft/braft_state_machine.h
src/dtx/raft/braft_state_machine.cc

// CMake 修改
find_package(braft REQUIRED)
target_link_libraries(cedar braft::braft)
```

### Phase 2: 与 MetaService 集成 (1 周)

- 替换 MemoryRaft
- 实现快照序列化 (Space/Partition/Node)
- 成员动态变更 API

### Phase 3: 多节点集群支持 (1 周)

- 配置文件支持多节点
- 节点发现与加入
- Leader 故障转移测试
