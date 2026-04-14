#include "cedar/dtx/meta_service_grpc.h"
namespace cedar {
namespace dtx {
MetaServiceGrpcImpl::MetaServiceGrpcImpl(MetadataService* meta_service) 
    : meta_service_(meta_service) {}
grpc::Status MetaServiceGrpcImpl::CreateSpace(grpc::ServerContext* context,
    const cedar::meta::CreateSpaceRequest* request,
    cedar::meta::CreateSpaceResponse* response) {
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
    auto result = meta_service_->GetSpacePartitionMap(request->space_name());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
    } else {
        auto* map = response->mutable_partition_map();
        map->set_space_name(result.value().space_name);
        map->set_num_partitions(result.value().num_partitions);
        map->set_replication_factor(result.value().replication_factor);
        map->set_version(result.value().version);
    }
    return grpc::Status::OK();
}
grpc::Status MetaServiceGrpcImpl::RegisterNode(grpc::ServerContext* context,
    const cedar::meta::RegisterNodeRequest* request,
    cedar::meta::RegisterNodeResponse* response) {
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
    return grpc::Status::OK();
}
grpc::Status MetaServiceGrpcImpl::GetAliveNodes(grpc::ServerContext* context,
    const cedar::meta::GetAliveNodesRequest* request,
    cedar::meta::GetAliveNodesResponse* response) {
    auto nodes = meta_service_->GetAliveNodes();
    response->set_success(true);
    for (const auto& node : nodes) {
        auto* info = response->add_nodes();
        info->set_node_id(node.node_id);
        info->set_address(node.address);
    }
    return grpc::Status::OK();
}
grpc::Status MetaServiceGrpcImpl::WatchPartitionMap(grpc::ServerContext* context,
    const cedar::meta::WatchPartitionMapRequest* request,
    grpc::ServerWriter<cedar::meta::PartitionMapChange>* writer) {
    // Simplified implementation - just keep connection open
    return grpc::Status::OK;
}
Status MetaServiceGrpcServer::Start(const std::string& listen_address, MetadataService* meta_service) {
    service_impl_ = std::make_unique<MetaServiceGrpcImpl>(meta_service);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
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
} // namespace dtx
} // namespace cedar
