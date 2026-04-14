# braft Integration Guide

## Overview

This guide describes how to integrate braft (Baidu Raft) for production-grade consensus in MetaD.

## Prerequisites

### Install braft

#### Option 1: Build from source (Recommended)

```bash
# Install dependencies (macOS)
brew install brpc protobuf gflags leveldb

# Clone and build braft
git clone https://github.com/brpc/braft.git
cd braft
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local
make -j$(nproc)
sudo make install
```

#### Option 2: Docker (for CI/CD)

```dockerfile
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y \
    build-essential cmake git \
    libbrpc-dev libprotobuf-dev \
    libleveldb-dev libgflags-dev

# Build braft
WORKDIR /tmp
RUN git clone https://github.com/brpc/braft.git && \
    cd braft && mkdir build && cd build && \
    cmake .. && make -j4 && make install
```

## CMake Configuration

Add to `CMakeLists.txt`:

```cmake
# Find braft
find_package(braft QUIET)

if(braft_FOUND)
    message(STATUS "braft found, enabling Raft consensus")
    add_definitions(-DCEDAR_WITH_BRAFT)
    
    set(CEDAR_DTX_SOURCES
        # ... existing sources ...
        src/dtx/raft/braft_node.cc
        src/dtx/raft/braft_state_machine.cc
    )
    
    list(APPEND CEDAR_LIBS braft::braft)
else()
    message(WARNING "braft not found, using MemoryRaft (NOT FOR PRODUCTION)")
endif()
```

## Implementation Steps

### Step 1: Implement State Machine

```cpp
// src/dtx/raft/braft_state_machine.cc
#include "cedar/dtx/raft/braft_node.h"
#include "cedar/dtx/meta_service.h"

namespace cedar {
namespace dtx {

void MetaRaftStateMachine::on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard closure_guard(iter.done());
        
        // Deserialize command
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        RaftCommand cmd;
        // ... deserialize ...
        
        // Apply to MetaService
        switch (cmd.type) {
            case RaftCommandType::kCreateSpace:
                meta_service_->ApplyCreateSpace(cmd.payload);
                break;
            case RaftCommandType::kUpdatePartition:
                meta_service_->ApplyUpdatePartition(cmd.payload);
                break;
            // ... etc
        }
        
        // Update applied index for snapshot
        last_applied_index_ = iter.index();
    }
}

void MetaRaftStateMachine::on_snapshot_save(
        braft::SnapshotWriter* writer, 
        braft::Closure* done) {
    // Save MetaService state to snapshot
    std::string snapshot_path = writer->get_path();
    snapshot_path += "/meta_snapshot";
    
    // Serialize all spaces, partitions, nodes
    meta_service_->SaveSnapshot(snapshot_path);
    
    // Add file to snapshot
    writer->add_file("meta_snapshot");
    
    if (done) {
        done->Run();
    }
}

int MetaRaftStateMachine::on_snapshot_load(
        braft::SnapshotReader* reader) {
    std::string snapshot_path = reader->get_path();
    snapshot_path += "/meta_snapshot";
    
    // Load state from snapshot
    return meta_service_->LoadSnapshot(snapshot_path);
}

void MetaRaftStateMachine::on_leader_start(int64_t term) {
    last_term_ = term;
    LOG(INFO) << "Node becomes leader, term=" << term;
    
    // Notify MetaService to enable write operations
    meta_service_->OnBecomeLeader();
}

void MetaRaftStateMachine::on_leader_stop(const butil::Status& status) {
    LOG(INFO) << "Node steps down from leader: " << status.error_str();
    
    // Notify MetaService to disable write operations
    meta_service_->OnStepDown();
}

}  // namespace dtx
}  // namespace cedar
```

### Step 2: Implement Node Wrapper

```cpp
// src/dtx/raft/braft_node.cc
#include "cedar/dtx/raft/braft_node.h"

namespace cedar {
namespace dtx {

class BRaftNode::Impl {
 public:
    std::unique_ptr<MetaRaftStateMachine> fsm_;
    std::unique_ptr<braft::Node> node_;
    braft::NodeOptions node_options_;
};

Status BRaftNode::Init(const Options& options, MetaService* meta_service) {
    impl_ = std::make_unique<Impl>();
    
    // Create state machine
    impl_->fsm_ = std::make_unique<MetaRaftStateMachine>(meta_service);
    
    // Configure Raft node
    impl_->node_options_.election_timeout_ms = options.election_timeout_ms;
    impl_->node_options_.fsm = impl_->fsm_.get();
    impl_->node_options_.node_owns_fsm = false;
    
    // Set up log storage
    impl_->node_options_.log_uri = "local://" + options.data_path + "/log";
    impl_->node_options_.raft_meta_uri = "local://" + options.data_path + "/meta";
    impl_->node_options_.snapshot_uri = "local://" + options.data_path + "/snapshot";
    
    // Parse initial configuration
    braft::Configuration conf;
    for (const auto& peer : options.initial_peers) {
        conf.add_peer(peer);
    }
    impl_->node_options_.initial_conf = conf;
    
    // Create and start node
    impl_->node_.reset(new braft::Node("MetaD", 
        butil::EndPoint(butil::IP_ANY, port)));
    
    if (impl_->node_->init(impl_->node_options_) != 0) {
        return Status::IOError("Failed to init braft node");
    }
    
    return Status::OK();
}

void BRaftNode::Shutdown() {
    if (impl_ && impl_->node_) {
        impl_->node_->shutdown(nullptr);
        impl_->node_->join();
    }
}

bool BRaftNode::IsLeader() const {
    return impl_ && impl_->node_ && 
           impl_->node_->is_leader();
}

Status BRaftNode::Propose(const RaftCommand& command) {
    if (!IsLeader()) {
        return Status::NotLeader("Not leader");
    }
    
    butil::IOBuf log;
    // Serialize command to log
    
    braft::Task task;
    task.data = &log;
    task.done = nullptr;  // Or use callback
    
    impl_->node_->apply(task);
    return Status::OK();
}

}  // namespace dtx
}  // namespace cedar
```

### Step 3: Integrate with MetaService

Modify `MetaService` to use `BRaftNode`:

```cpp
class MetaService {
 public:
  Status Initialize(const MetaServiceOptions& options) {
    if (options.enable_raft) {
      BRaftNode::Options raft_options;
      raft_options.node_id = options.node_id;
      raft_options.listen_address = options.raft_address;
      raft_options.data_path = options.data_path + "/raft";
      raft_options.initial_peers = options.initial_peers;
      
      raft_node_ = RaftNodeFactory::Create(raft_options, this);
      auto s = raft_node_->Init(raft_options, this);
      if (!s.ok()) return s;
    } else {
      // Fallback to MemoryRaft (development only)
      raft_ = std::make_unique<MemoryRaft>(...);
    }
    
    return Status::OK();
  }
  
  // Only leader can modify metadata
  Status CreateSpace(const SpaceDef& space) {
    if (raft_node_ && !raft_node_->IsLeader()) {
      return Status::NotLeader("Not leader, redirect to: " + 
          raft_node_->GetLeaderId());
    }
    
    // Propose to Raft
    RaftCommand cmd;
    cmd.type = RaftCommandType::kCreateSpace;
    cmd.payload = Serialize(space);
    
    return raft_node_->Propose(cmd);
  }
  
  // Called by StateMachine
  void ApplyCreateSpace(const std::string& payload) {
    SpaceDef space = Deserialize<SpaceDef>(payload);
    // Apply to in-memory state
    spaces_[space.name] = space;
  }
  
 private:
  std::unique_ptr<BRaftNode> raft_node_;
  std::unique_ptr<MemoryRaft> raft_;  // Fallback
};
```

## Testing

### Unit Tests

```cpp
TEST(BRaftNodeTest, LeaderElection) {
    // Start 3 nodes
    auto node1 = CreateTestNode(1, {2, 3});
    auto node2 = CreateTestNode(2, {1, 3});
    auto node3 = CreateTestNode(3, {1, 2});
    
    // Wait for election
    sleep(5);
    
    // Verify only one leader
    int leader_count = 0;
    if (node1->IsLeader()) leader_count++;
    if (node2->IsLeader()) leader_count++;
    if (node3->IsLeader()) leader_count++;
    
    EXPECT_EQ(leader_count, 1);
}

TEST(BRaftNodeTest, LogReplication) {
    auto cluster = CreateTestCluster(3);
    
    // Wait for leader
    auto leader = cluster.WaitForLeader();
    
    // Propose command
    RaftCommand cmd{RaftCommandType::kCreateSpace, "test_space"};
    ASSERT_TRUE(leader->Propose(cmd).ok());
    
    // Verify replication to followers
    sleep(1);
    for (auto& node : cluster.nodes) {
        EXPECT_EQ(node->GetAppliedIndex(), leader->GetAppliedIndex());
    }
}
```

### Integration Tests

```bash
# Start 3-node cluster
./metad --node_id=1 --raft_addr=127.0.0.1:9091 --peers=127.0.0.1:9092,127.0.0.1:9093 &
./metad --node_id=2 --raft_addr=127.0.0.1:9092 --peers=127.0.0.1:9091,127.0.0.1:9093 &
./metad --node_id=3 --raft_addr=127.0.0.1:9093 --peers=127.0.0.1:9091,127.0.0.1:9092 &

# Test leader election
sleep 5
./metad_client --command=get_leader

# Test data replication
./metad_client --command=create_space --name=test_space
./metad_client --command=get_space --name=test_space

# Kill leader and verify failover
kill -9 <leader_pid>
sleep 5
./metad_client --command=get_leader  # Should show new leader
```

## Deployment

### Configuration File

```yaml
# metad.conf
node_id: 1
raft_listen: "0.0.0.0:9090"
peers:
  - "192.168.1.1:9090"  # node 1
  - "192.168.1.2:9090"  # node 2
  - "192.168.1.3:9090"  # node 3
data_path: "/var/lib/cedar/metad"
election_timeout_ms: 5000
snapshot_interval_s: 3600
```

### Monitoring

```cpp
// Expose Raft metrics
class RaftMetrics {
  Gauge is_leader;
  Gauge current_term;
  Counter committed_index;
  Counter applied_index;
  Histogram commit_latency;
};
```

## Troubleshooting

### Common Issues

1. **Node fails to start**: Check log storage permissions
2. **No leader elected**: Verify network connectivity between peers
3. **Log replication slow**: Adjust snapshot interval, check disk I/O
4. **Memory usage high**: Enable log compaction, reduce snapshot retention

### Debug Commands

```bash
# Check node status
curl http://localhost:9090/raft/status

# View Raft logs
tail -f /var/lib/cedar/metad/raft/log/LOG

# Force snapshot
curl -X POST http://localhost:9090/raft/snapshot
```
