#include "cedar/dtx/meta_service_grpc.h"

#include "cedar/dtx/raft/grpc_tls.h"
#include <grpcpp/grpcpp.h>
#include <iostream>

namespace cedar {
namespace dtx {

// =============================================================================
// MetaServiceGrpcImpl - Server
// =============================================================================

MetaServiceGrpcImpl::MetaServiceGrpcImpl(MetadataService* meta_service) 
    : meta_service_(meta_service) {}

grpc::Status MetaServiceGrpcImpl::CreateSpace(grpc::ServerContext* context,
    const cedar::meta::CreateSpaceRequest* request,
    cedar::meta::CreateSpaceResponse* response) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    SpaceDef space;
    space.name = request->space().name();
    space.partition_num = request->space().partition_num();
    space.replica_factor = request->space().replica_factor();
    auto status = meta_service_->CreateSpace(space);
    response->set_success(status.ok());
    if (!status.ok()) response->set_error_msg(status.ToString());
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetSpace(grpc::ServerContext* context,
    const cedar::meta::GetSpaceRequest* request,
    cedar::meta::GetSpaceResponse* response) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto result = meta_service_->GetSpace(request->space_name());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
    } else {
        auto* space = response->mutable_space();
        space->set_name(result.value().name);
        space->set_partition_num(result.value().partition_num);
        space->set_replica_factor(result.value().replica_factor);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetPartitionAssignment(grpc::ServerContext* context,
    const cedar::meta::GetPartitionAssignmentRequest* request,
    cedar::meta::GetPartitionAssignmentResponse* response) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto result = meta_service_->GetPartitionAssignment(request->space_name(), 
                                                        request->partition_id());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
    } else {
        auto* assign = response->mutable_assignment();
        assign->set_partition_id(result.value().partition_id);
        assign->set_space_name(result.value().space_name);
        assign->set_leader_node(result.value().leader_node);
        for (auto nid : result.value().follower_nodes) {
            assign->add_follower_nodes(nid);
        }
        assign->set_version(result.value().version);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetSpacePartitionMap(grpc::ServerContext* context,
    const cedar::meta::GetSpacePartitionMapRequest* request,
    cedar::meta::GetSpacePartitionMapResponse* response) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto result = meta_service_->GetSpacePartitionMap(request->space_name());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
    } else {
        auto* map = response->mutable_partition_map();
        const auto& pmap = result.value();
        map->set_space_name(pmap.space_name);
        map->set_num_partitions(pmap.num_partitions);
        map->set_replication_factor(pmap.replication_factor);
        map->set_version(pmap.version);
        // Fill assignments
        for (const auto& [pid, assign] : pmap.assignments) {
            auto& proto_assign = (*map->mutable_assignments())[pid];
            proto_assign.set_partition_id(assign.partition_id);
            proto_assign.set_space_name(assign.space_name);
            proto_assign.set_leader_node(assign.leader_node);
            for (NodeID follower : assign.follower_nodes) {
                proto_assign.add_follower_nodes(follower);
            }
            proto_assign.set_version(assign.version);
        }
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::RegisterNode(grpc::ServerContext* context,
    const cedar::meta::RegisterNodeRequest* request,
    cedar::meta::RegisterNodeResponse* response) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    NodeInfo info;
    info.node_id = request->node_info().node_id();
    info.address = request->node_info().address();
    info.data_path = request->node_info().data_path();
    auto status = meta_service_->RegisterNode(info);
    response->set_success(status.ok());
    if (!status.ok()) response->set_error_msg(status.ToString());
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::Heartbeat(grpc::ServerContext* context,
    const cedar::meta::HeartbeatRequest* request,
    cedar::meta::HeartbeatResponse* response) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    NodeStatus status;
    status.node_id = request->status().node_id();
    status.cpu_usage_percent = request->status().cpu_usage_percent();
    status.timestamp = std::chrono::system_clock::now();
    auto s = meta_service_->Heartbeat(status);
    response->set_success(s.ok());
    if (!s.ok()) response->set_error_msg(s.ToString());
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetNode(grpc::ServerContext* context,
    const cedar::meta::GetNodeRequest* request,
    cedar::meta::GetNodeResponse* response) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto result = meta_service_->GetNode(request->node_id());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
    } else {
        auto* info = response->mutable_node_info();
        info->set_node_id(result.value().node_id);
        info->set_address(result.value().address);
        info->set_state(result.value().state == NodeInfo::State::kOnline ? "ONLINE" : "OFFLINE");
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetAliveNodes(grpc::ServerContext* context,
    const cedar::meta::GetAliveNodesRequest* request,
    cedar::meta::GetAliveNodesResponse* response) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    (void)request;
    auto nodes = meta_service_->GetAliveNodes();
    response->set_success(true);
    for (const auto& node : nodes) {
        auto* info = response->add_nodes();
        info->set_node_id(node.node_id);
        info->set_address(node.address);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::WatchPartitionMap(grpc::ServerContext* context,
    const cedar::meta::WatchPartitionMapRequest* request,
    grpc::ServerWriter<cedar::meta::PartitionMapChange>* writer) {
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    (void)request;
    (void)writer;
    // Simplified implementation - just keep connection open
    return grpc::Status::OK;
}

// =============================================================================
// MetaServiceGrpcServer
// =============================================================================

Status MetaServiceGrpcServer::Start(const std::string& listen_address, MetadataService* meta_service) {
    service_impl_ = std::make_unique<MetaServiceGrpcImpl>(meta_service);
    grpc::ServerBuilder builder;
    auto server_creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentialsFromEnv();
    builder.AddListeningPort(listen_address, server_creds);
    builder.RegisterService(service_impl_.get());
    server_ = builder.BuildAndStart();
    if (!server_) {
        return Status::IOError("Failed to start gRPC server");
    }
    return Status::OK();
}

Status MetaServiceGrpcServer::Stop() {
    if (server_) {
        server_->Shutdown();
    }
    return Status::OK();
}

void MetaServiceGrpcServer::Wait() {
    if (server_) {
        server_->Wait();
    }
}

// =============================================================================
// MetaServiceGrpcClient
// =============================================================================

MetaServiceGrpcClient::MetaServiceGrpcClient() = default;

MetaServiceGrpcClient::~MetaServiceGrpcClient() {
    health_monitor_running_.store(false);
    if (health_monitor_thread_.joinable()) {
        health_monitor_thread_.join();
    }
}

Status MetaServiceGrpcClient::Connect(const std::vector<std::string>& meta_addresses) {
    // Stop existing monitor if any
    if (health_monitor_running_.exchange(false)) {
        if (health_monitor_thread_.joinable()) {
            health_monitor_thread_.join();
        }
    }

    std::unique_lock<std::shared_mutex> lock(stub_mutex_);
    meta_addresses_ = meta_addresses;

    if (meta_addresses_.empty()) {
        return Status::InvalidArgument("No meta addresses provided");
    }

    // Try each address until one responds to GetAliveNodes
    for (size_t i = 0; i < meta_addresses_.size(); ++i) {
        current_index_ = i;
        channel_ = grpc::CreateChannel(
            meta_addresses_[i],
            cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv());
        stub_ = cedar::meta::MetaService::NewStub(channel_);

        cedar::meta::GetAliveNodesRequest req;
        cedar::meta::GetAliveNodesResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        auto status = stub_->GetAliveNodes(&ctx, req, &resp);
        if (status.ok() && resp.success()) {
            lock.unlock();
            // Start background health monitor
            health_monitor_running_ = true;
            health_monitor_thread_ = std::thread(&MetaServiceGrpcClient::HealthMonitorLoop, this);
            return Status::OK();
        }
    }

    // None responded — keep the last stub so TryReconnect can cycle
    lock.unlock();
    health_monitor_running_ = true;
    health_monitor_thread_ = std::thread(&MetaServiceGrpcClient::HealthMonitorLoop, this);
    return Status::IOError("All MetaD nodes unreachable at connect time");
}

StatusOr<NodeID> MetaServiceGrpcClient::GetPartitionLeader(const std::string& space_name, 
                                                            PartitionID partition_id) {
    auto assign = GetPartitionAssignment(space_name, partition_id);
    if (!assign.ok()) {
        return assign.status();
    }
    return assign.value().leader_node;
}

StatusOr<NodeID> MetaServiceGrpcClient::GetRouteForKey(const std::string& space_name,
                                                        const CedarKey& key) {
    auto map = GetSpacePartitionMap(space_name);
    if (!map.ok()) {
        return map.status();
    }
    PartitionID pid = map.value().GetPartitionForKey(key);
    return GetPartitionLeader(space_name, pid);
}

void MetaServiceGrpcClient::RefreshCache(const std::string& space_name) {
    (void)space_name;
    // TODO: implement cache
}

StatusOr<PartitionAssignment> MetaServiceGrpcClient::GetPartitionAssignment(
    const std::string& space_name, PartitionID partition_id) {
    auto stub = GetStub();
    if (!stub) return Status::IOError("Not connected to MetaD");
    
    cedar::meta::GetPartitionAssignmentRequest request;
    request.set_space_name(space_name);
    request.set_partition_id(partition_id);
    
    cedar::meta::GetPartitionAssignmentResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto grpc_status = stub->GetPartitionAssignment(&context, request, &response);
    if (!grpc_status.ok()) {
        auto reconnect = TryReconnect();
        if (!reconnect.ok()) {
            return Status::IOError("MetaD RPC failed and reconnect failed: " + grpc_status.error_message());
        }
        stub = GetStub();
        if (!stub) return Status::IOError("Not connected to MetaD after reconnect");
        grpc::ClientContext context2;
        context2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        grpc_status = stub->GetPartitionAssignment(&context2, request, &response);
        if (!grpc_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + grpc_status.error_message());
        }
    }
    if (!response.success()) {
        return Status::NotFound(response.error_msg());
    }
    
    PartitionAssignment assign;
    assign.partition_id = static_cast<PartitionID>(response.assignment().partition_id());
    assign.space_name = response.assignment().space_name();
    assign.leader_node = static_cast<NodeID>(response.assignment().leader_node());
    for (int i = 0; i < response.assignment().follower_nodes_size(); ++i) {
        assign.follower_nodes.push_back(static_cast<NodeID>(response.assignment().follower_nodes(i)));
    }
    assign.version = response.assignment().version();
    return assign;
}

StatusOr<SpacePartitionMap> MetaServiceGrpcClient::GetSpacePartitionMap(const std::string& space_name) {
    auto stub = GetStub();
    if (!stub) return Status::IOError("Not connected to MetaD");
    
    cedar::meta::GetSpacePartitionMapRequest request;
    request.set_space_name(space_name);
    
    cedar::meta::GetSpacePartitionMapResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto grpc_status = stub->GetSpacePartitionMap(&context, request, &response);
    if (!grpc_status.ok()) {
        auto reconnect = TryReconnect();
        if (!reconnect.ok()) {
            return Status::IOError("MetaD RPC failed and reconnect failed: " + grpc_status.error_message());
        }
        stub = GetStub();
        if (!stub) return Status::IOError("Not connected to MetaD after reconnect");
        grpc::ClientContext context2;
        context2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        grpc_status = stub->GetSpacePartitionMap(&context2, request, &response);
        if (!grpc_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + grpc_status.error_message());
        }
    }
    if (!response.success()) {
        return Status::NotFound(response.error_msg());
    }
    
    SpacePartitionMap map;
    map.space_name = response.partition_map().space_name();
    map.num_partitions = static_cast<uint32_t>(response.partition_map().num_partitions());
    map.replication_factor = static_cast<uint32_t>(response.partition_map().replication_factor());
    map.version = response.partition_map().version();
    return map;
}

StatusOr<NodeInfo> MetaServiceGrpcClient::GetNode(NodeID node_id) {
    auto stub = GetStub();
    if (!stub) return Status::IOError("Not connected to MetaD");
    
    cedar::meta::GetNodeRequest request;
    request.set_node_id(node_id);
    
    cedar::meta::GetNodeResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto grpc_status = stub->GetNode(&context, request, &response);
    if (!grpc_status.ok()) {
        auto reconnect = TryReconnect();
        if (!reconnect.ok()) {
            return Status::IOError("MetaD RPC failed and reconnect failed: " + grpc_status.error_message());
        }
        stub = GetStub();
        if (!stub) return Status::IOError("Not connected to MetaD after reconnect");
        grpc::ClientContext context2;
        context2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        grpc_status = stub->GetNode(&context2, request, &response);
        if (!grpc_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + grpc_status.error_message());
        }
    }
    if (!response.success()) {
        return Status::NotFound(response.error_msg());
    }
    
    NodeInfo info;
    info.node_id = static_cast<NodeID>(response.node_info().node_id());
    info.address = response.node_info().address();
    info.state = (response.node_info().state() == "ONLINE") ? NodeInfo::State::kOnline : NodeInfo::State::kOffline;
    return info;
}

Status MetaServiceGrpcClient::RegisterNode(const NodeInfo& info) {
    auto stub = GetStub();
    if (!stub) return Status::IOError("Not connected to MetaD");
    
    cedar::meta::RegisterNodeRequest request;
    auto* node_info = request.mutable_node_info();
    node_info->set_node_id(info.node_id);
    node_info->set_address(info.address);
    node_info->set_data_path(info.data_path);
    
    cedar::meta::RegisterNodeResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto grpc_status = stub->RegisterNode(&context, request, &response);
    if (!grpc_status.ok()) {
        auto reconnect = TryReconnect();
        if (!reconnect.ok()) {
            return Status::IOError("MetaD RPC failed and reconnect failed: " + grpc_status.error_message());
        }
        stub = GetStub();
        if (!stub) return Status::IOError("Not connected to MetaD after reconnect");
        grpc::ClientContext context2;
        context2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        grpc_status = stub->RegisterNode(&context2, request, &response);
        if (!grpc_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + grpc_status.error_message());
        }
    }
    if (!response.success()) {
        return Status::IOError(response.error_msg());
    }
    return Status::OK();
}

Status MetaServiceGrpcClient::Heartbeat(const NodeStatus& status) {
    auto stub = GetStub();
    if (!stub) return Status::IOError("Not connected to MetaD");
    
    cedar::meta::HeartbeatRequest request;
    auto* node_status = request.mutable_status();
    node_status->set_node_id(status.node_id);
    node_status->set_cpu_usage_percent(status.cpu_usage_percent);
    
    cedar::meta::HeartbeatResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto grpc_status = stub->Heartbeat(&context, request, &response);
    if (!grpc_status.ok()) {
        auto reconnect = TryReconnect();
        if (!reconnect.ok()) {
            return Status::IOError("MetaD RPC failed and reconnect failed: " + grpc_status.error_message());
        }
        stub = GetStub();
        if (!stub) return Status::IOError("Not connected to MetaD after reconnect");
        grpc::ClientContext context2;
        context2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        grpc_status = stub->Heartbeat(&context2, request, &response);
        if (!grpc_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + grpc_status.error_message());
        }
    }
    if (!response.success()) {
        return Status::IOError(response.error_msg());
    }
    return Status::OK();
}

void MetaServiceGrpcClient::WatchPartitionMap(const std::string& space_name,
                                              std::function<void(const PartitionMapChange&)> callback) {
    (void)space_name;
    (void)callback;
    // TODO: implement watch with streaming RPC
}

std::shared_ptr<cedar::meta::MetaService::Stub> MetaServiceGrpcClient::GetStub() {
    std::shared_lock<std::shared_mutex> lock(stub_mutex_);
    return stub_;
}

Status MetaServiceGrpcClient::TryReconnect() {
    std::vector<std::string> addresses;
    size_t start_index;
    {
        std::shared_lock<std::shared_mutex> lock(stub_mutex_);
        if (meta_addresses_.empty()) {
            return Status::IOError("No meta addresses configured");
        }
        addresses = meta_addresses_;
        start_index = current_index_.load();
    }

    for (size_t i = 0; i < addresses.size(); ++i) {
        size_t idx = (start_index + i + 1) % addresses.size();
        auto channel = grpc::CreateChannel(addresses[idx], cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv());
        auto stub = cedar::meta::MetaService::NewStub(channel);
        // Quick health check via GetAliveNodes
        cedar::meta::GetAliveNodesRequest req;
        cedar::meta::GetAliveNodesResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        auto status = stub->GetAliveNodes(&ctx, req, &resp);
        if (status.ok() && resp.success()) {
            std::unique_lock<std::shared_mutex> lock(stub_mutex_);
            channel_ = std::move(channel);
            stub_ = std::move(stub);
            current_index_ = idx;
            return Status::OK();
        }
    }
    return Status::IOError("All meta nodes unreachable");
}

void MetaServiceGrpcClient::HealthMonitorLoop() {
    while (health_monitor_running_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        if (!health_monitor_running_.load()) break;

        auto stub = GetStub();
        if (!stub) continue;

        cedar::meta::GetAliveNodesRequest req;
        cedar::meta::GetAliveNodesResponse resp;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        auto status = stub->GetAliveNodes(&ctx, req, &resp);

        if (!status.ok() || !resp.success()) {
            std::cerr << "[MetaServiceGrpcClient] Health check failed, triggering reconnect"
                      << std::endl;
            auto reconnect = TryReconnect();
            if (!reconnect.ok()) {
                std::cerr << "[MetaServiceGrpcClient] Reconnect failed: "
                          << reconnect.ToString() << std::endl;
            }
        }
    }
}

} // namespace dtx
} // namespace cedar
