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
// Meta Service gRPC 实现
// =============================================================================

#ifndef CEDAR_DTX_META_SERVICE_GRPC_H_
#define CEDAR_DTX_META_SERVICE_GRPC_H_

#include <grpcpp/grpcpp.h>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include "cedar/coordinator/location_table.h"
#include "cedar/dtx/meta_service.h"
#include "meta_service.grpc.pb.h"

namespace cedar {
namespace dtx {

/**
 * @brief MetaService gRPC 服务实现
 */
class MetaServiceGrpcImpl final : public cedar::meta::MetaService::Service {
public:
    explicit MetaServiceGrpcImpl(MetadataService* meta_service);
    
    // Space 管理
    grpc::Status CreateSpace(grpc::ServerContext* context,
                             const cedar::meta::CreateSpaceRequest* request,
                             cedar::meta::CreateSpaceResponse* response) override;
    
    grpc::Status GetSpace(grpc::ServerContext* context,
                          const cedar::meta::GetSpaceRequest* request,
                          cedar::meta::GetSpaceResponse* response) override;
    
    // 分区管理
    grpc::Status GetPartitionAssignment(grpc::ServerContext* context,
                                        const cedar::meta::GetPartitionAssignmentRequest* request,
                                        cedar::meta::GetPartitionAssignmentResponse* response) override;
    
    grpc::Status GetSpacePartitionMap(grpc::ServerContext* context,
                                      const cedar::meta::GetSpacePartitionMapRequest* request,
                                      cedar::meta::GetSpacePartitionMapResponse* response) override;
    
    // 节点管理
    grpc::Status RegisterNode(grpc::ServerContext* context,
                              const cedar::meta::RegisterNodeRequest* request,
                              cedar::meta::RegisterNodeResponse* response) override;
    
    grpc::Status Heartbeat(grpc::ServerContext* context,
                           const cedar::meta::HeartbeatRequest* request,
                           cedar::meta::HeartbeatResponse* response) override;
    
    grpc::Status GetNode(grpc::ServerContext* context,
                         const cedar::meta::GetNodeRequest* request,
                         cedar::meta::GetNodeResponse* response) override;
    
    grpc::Status GetAliveNodes(grpc::ServerContext* context,
                               const cedar::meta::GetAliveNodesRequest* request,
                               cedar::meta::GetAliveNodesResponse* response) override;
    
    // Watch 接口 - 流式
    grpc::Status WatchPartitionMap(grpc::ServerContext* context,
                                   const cedar::meta::WatchPartitionMapRequest* request,
                                   grpc::ServerWriter<cedar::meta::PartitionMapChange>* writer) override;

    // Schema 管理
    grpc::Status CreateLabelSchema(grpc::ServerContext* context,
                                   const cedar::meta::CreateLabelSchemaRequest* request,
                                   cedar::meta::CreateLabelSchemaResponse* response) override;

    grpc::Status GetSchema(grpc::ServerContext* context,
                           const cedar::meta::GetSchemaRequest* request,
                           cedar::meta::GetSchemaResponse* response) override;

    // GCN Cache management
    grpc::Status LocateCache(grpc::ServerContext* context,
                             const cedar::meta::LocateCacheRequest* request,
                             cedar::meta::LocateCacheResponse* response) override;

    grpc::Status ReportCache(grpc::ServerContext* context,
                             const cedar::meta::ReportCacheRequest* request,
                             cedar::meta::ReportCacheResponse* response) override;

    grpc::Status GcnHeartbeat(grpc::ServerContext* context,
                              const cedar::meta::GcnHeartbeatRequest* request,
                              cedar::meta::GcnHeartbeatResponse* response) override;

private:
    MetadataService* meta_service_;
    coordinator::VertexLocationTable location_table_;
    
    // Active WatchPartitionMap streams
    struct WatchStream {
        std::mutex mutex;
        std::condition_variable cv;
        std::queue<cedar::meta::PartitionMapChange> pending_changes;
        bool cancelled = false;
    };
    mutable std::mutex watchers_mutex_;
    std::vector<std::weak_ptr<WatchStream>> active_watchers_;
    std::queue<cedar::meta::PartitionMapChange> pending_broadcasts_;
    
    void OnPartitionChange(const PartitionMapChange& change);
    
    // 类型转换 helpers
    SpaceDef FromProto(const cedar::meta::SpaceDef& proto);
    cedar::meta::SpaceDef ToProto(const SpaceDef& space);
    
    PartitionAssignment FromProto(const cedar::meta::PartitionAssignment& proto);
    cedar::meta::PartitionAssignment ToProto(const PartitionAssignment& assign);
    
    NodeInfo FromProto(const cedar::meta::NodeInfo& proto);
    cedar::meta::NodeInfo ToProto(const NodeInfo& info);
    
    NodeStatus FromProto(const cedar::meta::NodeStatus& proto);
};

/**
 * @brief MetaService gRPC 服务器
 */
class MetaServiceGrpcServer {
public:
    MetaServiceGrpcServer() = default;
    ~MetaServiceGrpcServer() = default;
    
    // 启动 gRPC 服务器
    Status Start(const std::string& listen_address, MetadataService* meta_service);
    
    // 停止服务器
    Status Stop();
    
    // 等待服务器关闭
    void Wait();

private:
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<MetaServiceGrpcImpl> service_impl_;
};

/**
 * @brief MetaService gRPC 客户端实现
 */
class MetaServiceGrpcClient : public MetaServiceClient {
public:
    MetaServiceGrpcClient();
    ~MetaServiceGrpcClient() override;
    
    // 连接到 MetaD 集群
    Status Connect(const std::vector<std::string>& meta_addresses) override;
    
    // 实现基类接口
    StatusOr<NodeID> GetPartitionLeader(const std::string& space_name, 
                                         PartitionID partition_id) override;
    
    StatusOr<NodeID> GetRouteForKey(const std::string& space_name, 
                                     const CedarKey& key) override;
    
    void RefreshCache(const std::string& space_name) override;
    
    StatusOr<PartitionAssignment> GetPartitionAssignment(
        const std::string& space_name, PartitionID partition_id) override;
    
    StatusOr<SpacePartitionMap> GetSpacePartitionMap(const std::string& space_name) override;
    
    StatusOr<NodeInfo> GetNode(NodeID node_id) override;
    
    StatusOr<std::vector<NodeInfo>> GetAliveNodes();
    
    Status RegisterNode(const NodeInfo& info) override;
    
    Status Heartbeat(const NodeStatus& status) override;
    
    void WatchPartitionMap(const std::string& space_name,
                          std::function<void(const PartitionMapChange&)> callback) override;

    // 获取可用的 stub
    std::shared_ptr<cedar::meta::MetaService::Stub> GetStub();

private:
    
    // 处理连接失败，尝试其他节点
    Status TryReconnect();
    
    std::thread health_monitor_thread_;
    std::atomic<bool> health_monitor_running_{false};
    void HealthMonitorLoop();
    
    std::vector<std::string> meta_addresses_;
    std::atomic<size_t> current_index_{0};
    
    std::shared_ptr<grpc::Channel> channel_;
    std::shared_ptr<cedar::meta::MetaService::Stub> stub_;
    mutable std::shared_mutex stub_mutex_;
};

} // namespace dtx
} // namespace cedar

#endif // CEDAR_DTX_META_SERVICE_GRPC_H_
