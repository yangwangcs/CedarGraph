// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// CedarGraph MetaD Service Handler
// =============================================================================
// gRPC service implementation for meta_service.proto
// Provides partition topology, node registration, and cluster state management

#ifndef CEDAR_SERVICE_META_SERVICE_HANDLER_H_
#define CEDAR_SERVICE_META_SERVICE_HANDLER_H_

#include <grpcpp/grpcpp.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "meta_service.grpc.pb.h"
#include "meta_service.pb.h"
#include "cedar/raft/partition_metadata_service.h"
#include "raft/hotspot_detector.h"
#include "partition_allocator.h"

namespace cedar {
namespace service {

// MetaD gRPC 服务实现
class MetaServiceHandler final : public cedar::meta::MetaService::Service {
 public:
  MetaServiceHandler();
  ~MetaServiceHandler() override;

  // 禁止拷贝和移动
  MetaServiceHandler(const MetaServiceHandler&) = delete;
  MetaServiceHandler& operator=(const MetaServiceHandler&) = delete;

  // 初始化
  cedar::Status Initialize(const std::string& data_dir);
  
  // 启动后台任务（如心跳检查）
  cedar::Status Start();
  
  // 停止
  cedar::Status Stop();

  // ========== gRPC 方法实现 ==========
  
  // 心跳 - StorageD/GraphD 定期上报状态
  grpc::Status Heartbeat(grpc::ServerContext* context,
                         const cedar::meta::HeartbeatRequest* request,
                         cedar::meta::HeartbeatResponse* response) override;

  // 节点注册
  grpc::Status RegisterNode(grpc::ServerContext* context,
                            const cedar::meta::RegisterNodeRequest* request,
                            cedar::meta::RegisterNodeResponse* response) override;

  // 获取单个分区信息
  grpc::Status GetPartitionAssignment(grpc::ServerContext* context,
                                      const cedar::meta::GetPartitionAssignmentRequest* request,
                                      cedar::meta::GetPartitionAssignmentResponse* response) override;

  // 获取完整分区映射
  grpc::Status GetSpacePartitionMap(grpc::ServerContext* context,
                                    const cedar::meta::GetSpacePartitionMapRequest* request,
                                    cedar::meta::GetSpacePartitionMapResponse* response) override;

  // 创建图空间
  grpc::Status CreateSpace(grpc::ServerContext* context,
                           const cedar::meta::CreateSpaceRequest* request,
                           cedar::meta::CreateSpaceResponse* response) override;

  // 获取图空间信息
  grpc::Status GetSpace(grpc::ServerContext* context,
                        const cedar::meta::GetSpaceRequest* request,
                        cedar::meta::GetSpaceResponse* response) override;

  // 获取节点信息
  grpc::Status GetNode(grpc::ServerContext* context,
                       const cedar::meta::GetNodeRequest* request,
                       cedar::meta::GetNodeResponse* response) override;

  // 获取所有存活节点
  grpc::Status GetAliveNodes(grpc::ServerContext* context,
                             const cedar::meta::GetAliveNodesRequest* request,
                             cedar::meta::GetAliveNodesResponse* response) override;

  // Watch 分区映射变更（流式）
  grpc::Status WatchPartitionMap(grpc::ServerContext* context,
                                 const cedar::meta::WatchPartitionMapRequest* request,
                                 grpc::ServerWriter<cedar::meta::PartitionMapChange>* writer) override;

 private:
  // 心跳检查线程
  void HeartbeatCheckLoop();
  
  // 检查节点是否超时
  bool IsNodeExpiredLocked(const std::string& node_id);
  
  // 从节点状态提取 NodeInfo
  cedar::meta::NodeInfo BuildNodeInfoLocked(uint32_t node_id);

  std::unique_ptr<raft::PartitionMetadataService> metadata_service_;
  std::unique_ptr<PartitionAllocator> partition_allocator_;
  std::unique_ptr<raft::HotspotDetector> hotspot_detector_;
  
  // 节点心跳记录 (node_id -> last heartbeat)
  struct HeartbeatRecord {
    cedar::meta::NodeStatus last_status;
    std::chrono::steady_clock::time_point last_update;
    bool is_online = true;
  };
  
  std::mutex nodes_mutex_;
  std::unordered_map<uint32_t, HeartbeatRecord> node_heartbeats_;
  
  // 超时配置
  std::chrono::seconds heartbeat_timeout_{30};
  std::chrono::seconds heartbeat_check_interval_{10};
  
  std::atomic<bool> running_{false};
  std::thread heartbeat_thread_;
};

}  // namespace service
}  // namespace cedar

#endif  // CEDAR_SERVICE_META_SERVICE_HANDLER_H_
