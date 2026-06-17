#include "cedar/dtx/meta_service_grpc.h"

#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/dtx/security.h"
#include <grpcpp/grpcpp.h>
#include <iostream>

namespace {

grpc::Status CheckAuth(grpc::ServerContext* context,
                       cedar::dtx::security::Permission perm) {
  auto* sm = cedar::dtx::security::SecurityManager::GetInstance();
  if (!sm || !sm->IsAuthEnabled() || !sm->GetAuthenticator()) return grpc::Status::OK;
  auto meta = context->client_metadata();
  auto it = meta.find("authorization");
  if (it == meta.end()) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Missing auth token");
  }
  auto st = sm->AuthenticateAndAuthorize(
      std::string(it->second.data(), it->second.size()), perm, "");
  if (!st.ok()) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, st.ToString());
  }
  return grpc::Status::OK;
}

grpc::StatusCode MapStatusToGrpcCode(const cedar::Status& status) {
    if (status.IsInvalidArgument()) return grpc::StatusCode::INVALID_ARGUMENT;
    if (status.IsNotFound()) return grpc::StatusCode::NOT_FOUND;
    if (status.IsResourceExhausted()) return grpc::StatusCode::RESOURCE_EXHAUSTED;
    if (status.IsNotSupportedError()) return grpc::StatusCode::UNIMPLEMENTED;
    if (status.IsIOError()) return grpc::StatusCode::UNAVAILABLE;
    return grpc::StatusCode::INTERNAL;
}

 cedar::dtx::PartitionAssignment PartitionAssignmentFromProto(
    const cedar::meta::PartitionAssignment& proto) {
   cedar::dtx::PartitionAssignment assignment;
  assignment.partition_id = static_cast<cedar::dtx::PartitionID>(proto.partition_id());
  assignment.space_name = proto.space_name();
  assignment.leader_node = static_cast<cedar::dtx::NodeID>(proto.leader_node());
  for (int i = 0; i < proto.follower_nodes_size(); ++i) {
    assignment.follower_nodes.push_back(
        static_cast<cedar::dtx::NodeID>(proto.follower_nodes(i)));
  }
  assignment.version = proto.version();
  return assignment;
}

}  // namespace

namespace cedar {
namespace dtx {

// =============================================================================
// MetaServiceGrpcImpl - Server
// =============================================================================

MetaServiceGrpcImpl::MetaServiceGrpcImpl(MetadataService* meta_service) 
    : meta_service_(meta_service) {
    if (meta_service_) {
        meta_service_->WatchPartitionMap("", [this](const PartitionMapChange& change) {
            this->OnPartitionChange(change);
        });
    }
}

grpc::Status MetaServiceGrpcImpl::CreateSpace(grpc::ServerContext* context,
    const cedar::meta::CreateSpaceRequest* request,
    cedar::meta::CreateSpaceResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    if (request->space().name().empty()) {
        response->set_success(false);
        response->set_error_msg("Space name cannot be empty");
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Space name cannot be empty");
    }
    SpaceDef space;
    space.name = request->space().name();
    space.partition_num = request->space().partition_num();
    space.replica_factor = request->space().replica_factor();
    auto alive_nodes = meta_service_->GetAliveNodes();
    if (space.replica_factor > alive_nodes.size()) {
        response->set_success(false);
        response->set_error_msg("Replica factor exceeds alive node count");
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Replica factor exceeds alive node count");
    }
    auto status = meta_service_->CreateSpace(space);
    response->set_success(status.ok());
    if (!status.ok()) {
        response->set_error_msg(status.ToString());
        return grpc::Status(MapStatusToGrpcCode(status), status.ToString());
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetSpace(grpc::ServerContext* context,
    const cedar::meta::GetSpaceRequest* request,
    cedar::meta::GetSpaceResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto result = meta_service_->GetSpace(request->space_name());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
        return grpc::Status(MapStatusToGrpcCode(result.status()), result.status().ToString());
    }
    auto* space = response->mutable_space();
    space->set_name(result.value().name);
    space->set_partition_num(result.value().partition_num);
    space->set_replica_factor(result.value().replica_factor);
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetPartitionAssignment(grpc::ServerContext* context,
    const cedar::meta::GetPartitionAssignmentRequest* request,
    cedar::meta::GetPartitionAssignmentResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto result = meta_service_->GetPartitionAssignment(request->space_name(),
                                                        request->partition_id());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
        return grpc::Status(MapStatusToGrpcCode(result.status()), result.status().ToString());
    }
    auto* assign = response->mutable_assignment();
    assign->set_partition_id(result.value().partition_id);
    assign->set_space_name(result.value().space_name);
    assign->set_leader_node(result.value().leader_node);
    for (auto nid : result.value().follower_nodes) {
        assign->add_follower_nodes(nid);
    }
    assign->set_version(result.value().version);
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::UpdatePartitionAssignment(grpc::ServerContext* context,
    const cedar::meta::UpdatePartitionAssignmentRequest* request,
    cedar::meta::UpdatePartitionAssignmentResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto proto = request->assignment();
    cedar::dtx::PartitionAssignment assignment;
    assignment.partition_id = static_cast<cedar::dtx::PartitionID>(proto.partition_id());
    assignment.space_name = proto.space_name();
    assignment.leader_node = static_cast<cedar::dtx::NodeID>(proto.leader_node());
    for (int i = 0; i < proto.follower_nodes_size(); ++i) {
        assignment.follower_nodes.push_back(
            static_cast<cedar::dtx::NodeID>(proto.follower_nodes(i)));
    }
    assignment.version = proto.version();
    auto status = meta_service_->UpdatePartitionAssignment(assignment);
    response->set_success(status.ok());
    if (!status.ok()) {
        response->set_error_msg(status.ToString());
        return grpc::Status(MapStatusToGrpcCode(status), status.ToString());
    }
    response->set_version(assignment.version);
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetSpacePartitionMap(grpc::ServerContext* context,
    const cedar::meta::GetSpacePartitionMapRequest* request,
    cedar::meta::GetSpacePartitionMapResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto result = meta_service_->GetSpacePartitionMap(request->space_name());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
        return grpc::Status(MapStatusToGrpcCode(result.status()), result.status().ToString());
    }
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
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::RegisterNode(grpc::ServerContext* context,
    const cedar::meta::RegisterNodeRequest* request,
    cedar::meta::RegisterNodeResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    
    // Leader check: redirect to leader if this is a follower
    if (!meta_service_->IsLeader()) {
        std::string leader_addr = meta_service_->GetLeaderAddress();
        response->set_success(false);
        response->set_error_msg("Not a leader");
        if (!leader_addr.empty()) {
            response->set_leader_address(leader_addr);
        }
        return grpc::Status::OK;
    }
    
    NodeInfo info;
    info.node_id = request->node_info().node_id();
    info.address = request->node_info().address();
    info.data_path = request->node_info().data_path();
    auto status = meta_service_->RegisterNode(info);
    response->set_success(status.ok());
    if (!status.ok()) {
        response->set_error_msg(status.ToString());
        return grpc::Status(MapStatusToGrpcCode(status), status.ToString());
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::Heartbeat(grpc::ServerContext* context,
    const cedar::meta::HeartbeatRequest* request,
    cedar::meta::HeartbeatResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kMonitor); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    NodeStatus status;
    status.node_id = request->status().node_id();
    status.cpu_usage_percent = request->status().cpu_usage_percent();
    status.memory_usage_percent = request->status().memory_usage_percent();
    status.disk_usage_percent = request->status().disk_usage_percent();
    status.qps = request->status().qps();
    status.latency_ms = request->status().latency_ms();
    for (int i = 0; i < request->status().leader_partitions_size(); ++i) {
        status.leader_partitions.push_back(request->status().leader_partitions(i));
    }
    for (int i = 0; i < request->status().follower_partitions_size(); ++i) {
        status.follower_partitions.push_back(request->status().follower_partitions(i));
    }
    status.timestamp = std::chrono::system_clock::from_time_t(
        static_cast<time_t>(request->status().timestamp_unix()));
    auto s = meta_service_->Heartbeat(status);
    response->set_success(s.ok());
    if (!s.ok()) {
        response->set_error_msg(s.ToString());
        return grpc::Status(MapStatusToGrpcCode(s), s.ToString());
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetNode(grpc::ServerContext* context,
    const cedar::meta::GetNodeRequest* request,
    cedar::meta::GetNodeResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto result = meta_service_->GetNode(request->node_id());
    response->set_success(result.ok());
    if (!result.ok()) {
        response->set_error_msg(result.status().ToString());
        return grpc::Status(MapStatusToGrpcCode(result.status()), result.status().ToString());
    }
    auto* info = response->mutable_node_info();
    info->set_node_id(result.value().node_id);
    info->set_address(result.value().address);
    info->set_state(result.value().state == NodeInfo::State::kOnline ? "ONLINE" : "OFFLINE");
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetAliveNodes(grpc::ServerContext* context,
    const cedar::meta::GetAliveNodesRequest* request,
    cedar::meta::GetAliveNodesResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
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


void MetaServiceGrpcImpl::OnPartitionChange(const PartitionMapChange& change) {
    cedar::meta::PartitionMapChange proto_change;
    proto_change.set_space_name(change.space_name);
    proto_change.set_partition_id(change.partition_id);
    switch (change.change_type) {
        case PartitionChangeType::kLeaderChanged:
            proto_change.set_change_type("LEADER_CHANGED");
            break;
        case PartitionChangeType::kReplicaAdded:
            proto_change.set_change_type("REPLICA_ADDED");
            break;
        case PartitionChangeType::kReplicaRemoved:
            proto_change.set_change_type("REPLICA_REMOVED");
            break;
        case PartitionChangeType::kPartitionMigrated:
            proto_change.set_change_type("PARTITION_MIGRATED");
            break;
        default:
            std::cerr << "[MetaServiceGrpc] Unknown partition change type" << std::endl;
            break;
    }
    proto_change.set_old_leader(change.old_leader);
    proto_change.set_new_leader(change.new_leader);
    proto_change.set_version(change.version);
    proto_change.set_timestamp_unix(
        std::chrono::system_clock::to_time_t(change.timestamp));
    
    // Collect streams to notify, then notify outside the lock
    std::vector<std::shared_ptr<WatchStream>> to_notify;
    {
        std::lock_guard<std::mutex> lock(watchers_mutex_);
        bool has_active = false;
        for (auto it = active_watchers_.begin(); it != active_watchers_.end();) {
            auto stream = it->lock();
            if (!stream) {
                it = active_watchers_.erase(it);
                continue;
            }
            {
                std::lock_guard<std::mutex> stream_lock(stream->mutex);
                stream->pending_changes.push(proto_change);
            }
            to_notify.push_back(stream);
            has_active = true;
            ++it;
        }
        if (!has_active) {
            pending_broadcasts_.push(proto_change);
        }
    }
    // Notify outside the lock to avoid priority inversion
    for (auto& stream : to_notify) {
        stream->cv.notify_one();
    }
}

grpc::Status MetaServiceGrpcImpl::WatchPartitionMap(grpc::ServerContext* context,
    const cedar::meta::WatchPartitionMapRequest* request,
    grpc::ServerWriter<cedar::meta::PartitionMapChange>* writer) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    (void)request;
    
    auto stream = std::make_shared<WatchStream>();
    {
        std::lock_guard<std::mutex> lock(watchers_mutex_);
        active_watchers_.push_back(stream);
        // Drain any pending broadcasts that arrived before this watcher connected
        while (!pending_broadcasts_.empty()) {
            stream->pending_changes.push(pending_broadcasts_.front());
            pending_broadcasts_.pop();
        }
    }
    stream->cv.notify_one();
    
    while (!context->IsCancelled()) {
        std::unique_lock<std::mutex> lock(stream->mutex);
        bool has_change = stream->cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return !stream->pending_changes.empty() || stream->cancelled ||
                   context->IsCancelled();
        });
        
        if (context->IsCancelled() || stream->cancelled) break;
        
        if (has_change && !stream->pending_changes.empty()) {
            auto change = stream->pending_changes.front();
            stream->pending_changes.pop();
            lock.unlock();
            
            if (!writer->Write(change)) {
                break;
            }
        }
    }
    
    {
        std::lock_guard<std::mutex> lock(watchers_mutex_);
        stream->cancelled = true;
        for (auto it = active_watchers_.begin(); it != active_watchers_.end();) {
            if (it->lock() == stream) {
                it = active_watchers_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::CreateLabelSchema(grpc::ServerContext* context,
    const cedar::meta::CreateLabelSchemaRequest* request,
    cedar::meta::CreateLabelSchemaResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    LabelSchema schema;
    schema.name = request->schema().name();
    for (const auto& proto_prop : request->schema().properties()) {
        PropertyDef prop;
        prop.name = proto_prop.name();
        prop.type = proto_prop.type();
        prop.nullable = proto_prop.nullable();
        prop.indexed = proto_prop.indexed();
        schema.properties.push_back(prop);
    }
    for (const auto& idx : request->schema().indexes()) {
        schema.indexes.push_back(idx);
    }

    auto status = meta_service_->CreateLabelSchema(
        request->space_name(), schema);
    response->set_success(status.ok());
    if (!status.ok()) {
        response->set_error_msg(status.ToString());
        return grpc::Status(MapStatusToGrpcCode(status), status.ToString());
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GetSchema(grpc::ServerContext* context,
    const cedar::meta::GetSchemaRequest* request,
    cedar::meta::GetSchemaResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    std::vector<std::string> labels(request->labels().begin(), request->labels().end());
    auto schemas = meta_service_->GetSchema(request->space_name(), labels);

    for (const auto& schema : schemas) {
        auto* out = response->add_labels();
        out->set_name(schema.name);
        for (const auto& prop : schema.properties) {
            auto* out_prop = out->add_properties();
            out_prop->set_name(prop.name);
            out_prop->set_type(prop.type);
            out_prop->set_nullable(prop.nullable);
            out_prop->set_indexed(prop.indexed);
        }
        for (const auto& idx : schema.indexes) {
            out->add_indexes(idx);
        }
    }
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::ListSpaces(grpc::ServerContext* context,
    const cedar::meta::ListSpacesRequest* request,
    cedar::meta::ListSpacesResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    (void)request;

    auto space_names = meta_service_->ListSpaces();
    for (const auto& name : space_names) {
        auto space_result = meta_service_->GetSpace(name);
        if (space_result.ok()) {
            const auto& space_def = space_result.ValueOrDie();
            auto* out = response->add_spaces();
            out->set_name(space_def.name);
            out->set_partition_num(space_def.partition_num);
            out->set_replica_factor(space_def.replica_factor);
            auto created_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                space_def.created_at.time_since_epoch()).count();
            out->set_created_at_unix(created_ms);
        }
    }
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::ListLabels(grpc::ServerContext* context,
    const cedar::meta::ListLabelsRequest* request,
    cedar::meta::ListLabelsResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;

    auto schemas = meta_service_->GetSchema(request->space_name(), {});
    for (const auto& schema : schemas) {
        auto* out = response->add_labels();
        out->set_name(schema.name);
        for (const auto& prop : schema.properties) {
            auto* out_prop = out->add_properties();
            out_prop->set_name(prop.name);
            out_prop->set_type(prop.type);
            out_prop->set_nullable(prop.nullable);
            out_prop->set_indexed(prop.indexed);
        }
        for (const auto& idx : schema.indexes) {
            out->add_indexes(idx);
        }
    }
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::CreateIndex(grpc::ServerContext* context,
    const cedar::meta::CreateIndexRequest* request,
    cedar::meta::CreateIndexResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;

    IndexDef index;
    index.name = request->index().name();
    index.label_name = request->index().label_name();
    index.space_name = request->index().space_name();
    index.unique = request->index().unique();
    for (const auto& prop : request->index().properties()) {
        index.properties.push_back(prop);
    }

    auto status = meta_service_->CreateIndex(request->space_name(), index);
    if (!status.ok()) {
        response->set_success(false);
        response->set_error_msg(status.ToString());
    } else {
        response->set_success(true);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::DropIndex(grpc::ServerContext* context,
    const cedar::meta::DropIndexRequest* request,
    cedar::meta::DropIndexResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;

    auto status = meta_service_->DropIndex(request->space_name(), request->index_name());
    if (!status.ok()) {
        response->set_success(false);
        response->set_error_msg(status.ToString());
    } else {
        response->set_success(true);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::ListIndexes(grpc::ServerContext* context,
    const cedar::meta::ListIndexesRequest* request,
    cedar::meta::ListIndexesResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;

    auto indexes = meta_service_->ListIndexes(request->space_name(), request->label_name());
    for (const auto& index : indexes) {
        auto* out = response->add_indexes();
        out->set_name(index.name);
        out->set_label_name(index.label_name);
        out->set_space_name(index.space_name);
        out->set_unique(index.unique);
        for (const auto& prop : index.properties) {
            out->add_properties(prop);
        }
    }
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::LocateCache(grpc::ServerContext* context,
    const cedar::meta::LocateCacheRequest* request,
    cedar::meta::LocateCacheResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kRead); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    auto window = location_table_.Locate(request->entity_id(), request->query_time());
    if (window) {
        response->set_found(true);
        auto* cw = response->mutable_window();
        cw->set_entity_id(window->entity_id);
        cw->set_cached_from(window->cached_from);
        cw->set_cached_to(window->cached_to);
        cw->set_gcn_node_id(window->gcn_node_id);
        cw->set_version(window->version);
        cw->set_expire_at(window->expire_at);
    } else {
        response->set_found(false);
    }
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::ReportCache(grpc::ServerContext* context,
    const cedar::meta::ReportCacheRequest* request,
    cedar::meta::ReportCacheResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kWrite); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    coordinator::CacheWindow window;
    window.entity_id = request->window().entity_id();
    window.cached_from = request->window().cached_from();
    window.cached_to = request->window().cached_to();
    window.gcn_node_id = request->window().gcn_node_id();
    window.version = request->window().version();
    window.expire_at = request->window().expire_at();
    location_table_.ReportCache(window);
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status MetaServiceGrpcImpl::GcnHeartbeat(grpc::ServerContext* context,
    const cedar::meta::GcnHeartbeatRequest* request,
    cedar::meta::GcnHeartbeatResponse* response) {
    if (auto st = CheckAuth(context, cedar::dtx::security::Permission::kMonitor); !st.ok()) return st;
    if (context->IsCancelled()) return grpc::Status::CANCELLED;
    std::vector<coordinator::CacheWindow> windows;
    for (int i = 0; i < request->windows_size(); ++i) {
        const auto& w = request->windows(i);
        coordinator::CacheWindow window;
        window.entity_id = w.entity_id();
        window.cached_from = w.cached_from();
        window.cached_to = w.cached_to();
        window.gcn_node_id = w.gcn_node_id();
        window.version = w.version();
        window.expire_at = w.expire_at();
        windows.push_back(window);
    }
    location_table_.Heartbeat(windows);
    response->set_success(true);
    return grpc::Status::OK;
}

// =============================================================================
// MetaServiceGrpcServer
// =============================================================================

Status MetaServiceGrpcServer::Start(const std::string& listen_address, MetadataService* meta_service) {
    service_impl_ = std::make_unique<MetaServiceGrpcImpl>(meta_service);
    grpc::ServerBuilder builder;
    auto server_creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentialsFromEnv();
    if (!server_creds.ok()) {
        return Status::IOError("Failed to create server TLS credentials: " + server_creds.status().ToString());
    }
    builder.AddListeningPort(listen_address, server_creds.ValueOrDie());
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

// Template helper: if the server says "Not leader", reconnect and retry once.
template <typename Request, typename Response>
grpc::Status MetaServiceGrpcClient::RetryRpcOnNotLeader(
    std::shared_ptr<cedar::meta::MetaService::Stub>& stub,
    grpc::Status (cedar::meta::MetaService::Stub::*rpc)(grpc::ClientContext*, const Request&, Response*),
    const Request& request,
    Response* response,
    std::chrono::seconds deadline) {
  if (!response->success() &&
      response->error_msg().find("Not leader") != std::string::npos) {
    auto reconnect = TryReconnect();
    if (reconnect.ok()) {
      stub = GetStub();
      if (stub) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + deadline);
        return (stub.get()->*rpc)(&context, request, response);
      }
    }
  }
  return grpc::Status::OK;
}

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
        auto client_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
        if (!client_creds.ok()) {
            return Status::IOError("Failed to create client TLS credentials: " + client_creds.status().ToString());
        }
        channel_ = grpc::CreateChannel(
            meta_addresses_[i],
            client_creds.ValueOrDie());
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
    // Refresh partition map from MetaD. Caching requires adding a local
    // partition_cache_ member to MetaServiceGrpcClient.
    auto map = GetSpacePartitionMap(space_name);
    (void)map;  // In production: update local cache with map.value()
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
        auto retry_status = RetryRpcOnNotLeader(stub, &cedar::meta::MetaService::Stub::GetPartitionAssignment,
                                                request, &response, std::chrono::seconds(5));
        if (!retry_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + retry_status.error_message());
        }
        if (!response.success()) {
            return Status::NotFound(response.error_msg());
        }
    }
    
    return PartitionAssignmentFromProto(response.assignment());
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
        auto retry_status = RetryRpcOnNotLeader(stub, &cedar::meta::MetaService::Stub::GetSpacePartitionMap,
                                                request, &response, std::chrono::seconds(5));
        if (!retry_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + retry_status.error_message());
        }
        if (!response.success()) {
            return Status::NotFound(response.error_msg());
        }
    }
    
    SpacePartitionMap map;
    map.space_name = response.partition_map().space_name();
    map.num_partitions = static_cast<uint32_t>(response.partition_map().num_partitions());
    map.replication_factor = static_cast<uint32_t>(response.partition_map().replication_factor());
    map.version = response.partition_map().version();
    // Fill assignments
    for (const auto& [partition_id, assignment_pb] : response.partition_map().assignments()) {
        map.assignments[partition_id] = PartitionAssignmentFromProto(assignment_pb);
    }
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
        auto retry_status = RetryRpcOnNotLeader(stub, &cedar::meta::MetaService::Stub::GetNode,
                                                request, &response, std::chrono::seconds(5));
        if (!retry_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + retry_status.error_message());
        }
        if (!response.success()) {
            return Status::NotFound(response.error_msg());
        }
    }
    
    NodeInfo info;
    info.node_id = static_cast<NodeID>(response.node_info().node_id());
    info.address = response.node_info().address();
    info.state = (response.node_info().state() == "ONLINE") ? NodeInfo::State::kOnline : NodeInfo::State::kOffline;
    return info;
}

StatusOr<std::vector<NodeInfo>> MetaServiceGrpcClient::GetAliveNodes() {
    auto stub = GetStub();
    if (!stub) return Status::IOError("Not connected to MetaD");
    
    cedar::meta::GetAliveNodesRequest request;
    cedar::meta::GetAliveNodesResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    
    auto grpc_status = stub->GetAliveNodes(&context, request, &response);
    if (!grpc_status.ok()) {
        auto reconnect = TryReconnect();
        if (!reconnect.ok()) {
            return Status::IOError("MetaD RPC failed and reconnect failed: " + grpc_status.error_message());
        }
        stub = GetStub();
        if (!stub) return Status::IOError("Not connected to MetaD after reconnect");
        grpc::ClientContext context2;
        context2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        grpc_status = stub->GetAliveNodes(&context2, request, &response);
        if (!grpc_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + grpc_status.error_message());
        }
    }
    if (!response.success()) {
        auto retry_status = RetryRpcOnNotLeader(stub, &cedar::meta::MetaService::Stub::GetAliveNodes,
                                                request, &response, std::chrono::seconds(5));
        if (!retry_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + retry_status.error_message());
        }
        if (!response.success()) {
            return Status::IOError(response.error_msg());
        }
    }
    
    std::vector<NodeInfo> result;
    for (const auto& proto_node : response.nodes()) {
        NodeInfo node;
        node.node_id = static_cast<NodeID>(proto_node.node_id());
        node.address = proto_node.address();
        node.state = NodeInfo::State::kOnline;
        result.push_back(node);
    }
    return result;
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
        auto retry_status = RetryRpcOnNotLeader(stub, &cedar::meta::MetaService::Stub::RegisterNode,
                                                request, &response, std::chrono::seconds(5));
        if (!retry_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + retry_status.error_message());
        }
        if (!response.success()) {
            return Status::IOError(response.error_msg());
        }
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
        auto retry_status = RetryRpcOnNotLeader(stub, &cedar::meta::MetaService::Stub::Heartbeat,
                                                request, &response, std::chrono::seconds(5));
        if (!retry_status.ok()) {
            return Status::IOError("MetaD RPC failed: " + retry_status.error_message());
        }
        if (!response.success()) {
            return Status::IOError(response.error_msg());
        }
    }
    return Status::OK();
}

void MetaServiceGrpcClient::WatchPartitionMap(const std::string& space_name,
                                              std::function<void(const PartitionMapChange&)> callback) {
    (void)space_name;
    (void)callback;
    // Watch requires a streaming RPC (WatchPartitionMapStream) in the proto.
    // For now, callers should poll RefreshCache periodically.
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
        auto client_creds = cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnv();
        if (!client_creds.ok()) {
            continue;
        }
        auto channel = grpc::CreateChannel(addresses[idx], client_creds.ValueOrDie());
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
