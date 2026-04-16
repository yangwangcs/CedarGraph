// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#include "meta_service_handler.h"

#include <iostream>
#include <chrono>
#include <thread>

namespace cedar {
namespace service {

using namespace cedar::meta;
// 不引入 Status，使用完整的 grpc::Status 来避免冲突

MetaServiceHandler::MetaServiceHandler()
    : metadata_service_(std::make_unique<raft::PartitionMetadataService>()),
      partition_allocator_(std::make_unique<PartitionAllocator>(
          AllocationStrategy::LOAD_BALANCED)) {}

MetaServiceHandler::~MetaServiceHandler() {
  Stop();
}

Status MetaServiceHandler::Initialize(const std::string& data_dir) {
  // 初始化元数据服务
  raft::PartitionTopologyConfig config;
  // TODO: 设置 data_dir
  auto status = metadata_service_->Initialize(config);
  if (!status.ok()) {
    std::cerr << "Failed to initialize metadata service: " << status.ToString() << std::endl;
    return status;
  }
  
  // 初始化分区分配器
  partition_allocator_->SetTotalPartitions(65536);
  partition_allocator_->SetReplicationFactor(3);
  
  // 初始化热点检测器
  raft::HotspotDetectorConfig detector_config;
  detector_config.check_interval_ms = 5000;  // 5秒检测一次
  detector_config.qps_threshold = 10000;
  detector_config.cpu_threshold = 0.8;

  hotspot_detector_ = std::make_unique<raft::HotspotDetector>(detector_config);
  auto detector_status = hotspot_detector_->Start();
  if (!detector_status.ok()) {
    std::cerr << "[MetaD] Failed to start hotspot detector: " << detector_status.ToString() << std::endl;
  }

  std::cout << "[MetaD] Hotspot detector initialized" << std::endl;
  
  std::cout << "[MetaD] Metadata service initialized" << std::endl;
  std::cout << "[MetaD] Partition allocator initialized (65536 partitions, RF=3)" << std::endl;
  return cedar::Status::OK();
}

Status MetaServiceHandler::Start() {
  running_ = true;
  
  // 启动心跳检查线程
  heartbeat_thread_ = std::thread(&MetaServiceHandler::HeartbeatCheckLoop, this);
  
  std::cout << "[MetaD] Heartbeat checker started" << std::endl;
  return cedar::Status::OK();
}

Status MetaServiceHandler::Stop() {
  if (!running_.exchange(false)) {
    return cedar::Status::OK();
  }
  
  // 停止心跳检查线程
  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
  
  // 停止热点检测器
  if (hotspot_detector_) {
    hotspot_detector_->Stop();
  }
  
  // 关闭元数据服务
  if (metadata_service_) {
    metadata_service_->Shutdown();
  }
  
  std::cout << "[MetaD] Service stopped" << std::endl;
  return cedar::Status::OK();
}

// ========== gRPC 方法实现 ==========

grpc::Status MetaServiceHandler::Heartbeat(grpc::ServerContext* context,
                                           const HeartbeatRequest* request,
                                           HeartbeatResponse* response) {
  (void)context;
  
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  uint32_t node_id = request->status().node_id();
  
  HeartbeatRecord record;
  record.last_status = request->status();
  record.last_update = std::chrono::steady_clock::now();
  record.is_online = true;
  node_heartbeats_[node_id] = record;
  
  // 更新分区分配器的节点负载信息
  NodeLoadInfo load_info;
  load_info.node_id = node_id;
  load_info.cpu_usage = request->status().cpu_usage_percent();
  load_info.memory_usage = request->status().memory_usage_percent();
  load_info.disk_usage = request->status().disk_usage_percent();
  load_info.qps = request->status().qps();
  load_info.is_healthy = true;
  partition_allocator_->UpdateNodeLoad(node_id, load_info);
  
  // 记录节点负载到热点检测器
  if (hotspot_detector_) {
    hotspot_detector_->RecordCPU(request->status().node_id(), 
                                 request->status().cpu_usage_percent());
    
    // 记录分区访问（简化：假设所有 leader 分区都有访问）
    for (int i = 0; i < request->status().leader_partitions_size(); ++i) {
      hotspot_detector_->RecordAccess(request->status().leader_partitions(i), 
                                      false,  // 读操作
                                      request->status().qps() / 
                                      (request->status().leader_partitions_size() + 1));
    }
  }
  
  response->set_success(true);
  
  // TODO: 返回拓扑变更通知
  return grpc::Status::OK;
}

grpc::Status MetaServiceHandler::RegisterNode(grpc::ServerContext* context,
                                              const RegisterNodeRequest* request,
                                              RegisterNodeResponse* response) {
  (void)context;
  
  uint32_t node_id = request->node_info().node_id();
  const std::string& address = request->node_info().address();
  
  // 注册到元数据服务
  raft::StorageNodeMetadata node;
  node.node_id = std::to_string(node_id);
  node.address = address;
  
  auto status = metadata_service_->RegisterNode(node);
  
  // 注册到分区分配器
  if (status.ok()) {
    auto alloc_status = partition_allocator_->RegisterNode(node_id, address);
    if (!alloc_status.ok()) {
      std::cerr << "[MetaD] Failed to register node to partition allocator: " 
                << alloc_status.ToString() << std::endl;
    }
    
    // 如果是第一个节点，初始化分区分配
    static std::once_flag init_flag;
    std::call_once(init_flag, [this]() {
      std::cout << "[MetaD] First node registered, initializing partition allocation..." << std::endl;
      auto init_status = partition_allocator_->AllocateAllPartitions();
      if (!init_status.ok()) {
        std::cerr << "[MetaD] Failed to allocate partitions: " << init_status.ToString() << std::endl;
      }
    });
  }
  
  response->set_success(status.ok());
  if (!status.ok()) {
    response->set_error_msg(status.ToString());
  }
  
  std::cout << "[MetaD] Node registered: " << node_id 
            << " @ " << address << std::endl;
  
  return grpc::Status();
}

grpc::Status MetaServiceHandler::GetPartitionAssignment(grpc::ServerContext* context,
                                                         const GetPartitionAssignmentRequest* request,
                                                         GetPartitionAssignmentResponse* response) {
  (void)context;
  
  // 从分区分配器获取分配信息
  auto result = partition_allocator_->GetAllocation(request->partition_id());
  
  if (result.ok()) {
    auto* assignment = response->mutable_assignment();
    auto alloc = result.ValueOrDie();
    assignment->set_partition_id(alloc.partition_id);
    assignment->set_space_name(request->space_name());
    assignment->set_leader_node(alloc.leader_node);
    assignment->set_version(alloc.version);
    
    for (uint32_t follower : alloc.followers) {
      assignment->add_follower_nodes(follower);
    }
    
    response->set_success(true);
  } else {
    // 如果分配器中没有，尝试从元数据服务获取
    auto meta_result = metadata_service_->GetPartitionMetadata(
        request->space_name(), request->partition_id());
    
    if (meta_result.ok()) {
      auto* assignment = response->mutable_assignment();
      auto metadata = meta_result.ValueOrDie();
      assignment->set_partition_id(metadata.part_id);
      assignment->set_space_name(metadata.space_name);
      response->set_success(true);
    } else {
      response->set_success(false);
      response->set_error_msg(result.status().ToString());
    }
  }
  
  return grpc::Status();
}

grpc::Status MetaServiceHandler::GetSpacePartitionMap(grpc::ServerContext* context,
                                                      const GetSpacePartitionMapRequest* request,
                                                      GetSpacePartitionMapResponse* response) {
  (void)context;
  (void)request;
  
  // 从分区分配器获取所有分配
  auto allocs = partition_allocator_->GetAllAllocations();
  
  auto* partition_map = response->mutable_partition_map();
  partition_map->set_space_name(request->space_name());
  partition_map->set_num_partitions(65536);
  partition_map->set_replication_factor(3);
  
  for (const auto& alloc : allocs) {
    auto& assignment = (*partition_map->mutable_assignments())[alloc.partition_id];
    assignment.set_partition_id(alloc.partition_id);
    assignment.set_leader_node(alloc.leader_node);
    for (uint32_t follower : alloc.followers) {
      assignment.add_follower_nodes(follower);
    }
    assignment.set_version(alloc.version);
  }
  
  response->set_success(true);
  return grpc::Status();
}

grpc::Status MetaServiceHandler::CreateSpace(grpc::ServerContext* context,
                                             const CreateSpaceRequest* request,
                                             CreateSpaceResponse* response) {
  (void)context;
  
  auto status = metadata_service_->CreateSpace(
      request->space().name(),
      request->space().partition_num(),
      request->space().replica_factor());
  
  response->set_success(status.ok());
  if (!status.ok()) {
    response->set_error_msg(status.ToString());
  }
  
  std::cout << "[MetaD] Space created: " << request->space().name() 
            << " (partitions=" << request->space().partition_num() 
            << ", replicas=" << request->space().replica_factor() << ")" << std::endl;
  
  return grpc::Status();
}

grpc::Status MetaServiceHandler::GetSpace(grpc::ServerContext* context,
                                          const GetSpaceRequest* request,
                                          GetSpaceResponse* response) {
  (void)context;
  
  // TODO: 实现获取图空间
  response->set_success(false);
  response->set_error_msg("Not implemented");
  
  return grpc::Status();
}

grpc::Status MetaServiceHandler::GetNode(grpc::ServerContext* context,
                                         const GetNodeRequest* request,
                                         GetNodeResponse* response) {
  (void)context;
  
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  auto it = node_heartbeats_.find(request->node_id());
  if (it != node_heartbeats_.end()) {
    auto* node_info = response->mutable_node_info();
    *node_info = BuildNodeInfoLocked(request->node_id());
    response->set_success(true);
  } else {
    response->set_success(false);
    response->set_error_msg("Node not found");
  }
  
  return grpc::Status();
}

grpc::Status MetaServiceHandler::GetAliveNodes(grpc::ServerContext* context,
                                               const GetAliveNodesRequest* request,
                                               GetAliveNodesResponse* response) {
  (void)context;
  (void)request;
  
  std::lock_guard<std::mutex> lock(nodes_mutex_);
  
  for (const auto& [node_id, record] : node_heartbeats_) {
    if (record.is_online) {
      auto* node_info = response->add_nodes();
      *node_info = BuildNodeInfoLocked(node_id);
    }
  }
  
  response->set_success(true);
  return grpc::Status();
}

grpc::Status MetaServiceHandler::WatchPartitionMap(grpc::ServerContext* context,
                                                   const WatchPartitionMapRequest* request,
                                                   grpc::ServerWriter<PartitionMapChange>* writer) {
  (void)context;
  (void)request;
  (void)writer;
  
  // TODO: 实现流式分区映射变更通知
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Watch not implemented");
}

grpc::Status MetaServiceHandler::RegisterQueryD(grpc::ServerContext* context,
                                                const cedar::meta::RegisterQueryDRequest* request,
                                                cedar::meta::RegisterQueryDResponse* response) {
  (void)context;
  std::lock_guard<std::mutex> lock(nodes_mutex_);

  cedar::meta::NodeInfo info;
  info.set_node_id(request->node_id());
  info.set_address(request->listen_address());
  info.set_state("ONLINE");
  info.set_last_heartbeat_unix(
      std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());

  // Store as a queryd node in the heartbeat map
  HeartbeatRecord record;
  record.last_status.set_node_id(request->node_id());
  record.last_status.set_cpu_usage_percent(0.0);
  record.last_status.set_memory_usage_percent(0.0);
  record.last_update = std::chrono::steady_clock::now();
  record.is_online = true;
  node_heartbeats_[request->node_id()] = std::move(record);

  response->set_success(true);
  return grpc::Status::OK;
}

grpc::Status MetaServiceHandler::QueryDHeartbeat(grpc::ServerContext* context,
                                                 const cedar::meta::QueryDHeartbeatRequest* request,
                                                 cedar::meta::QueryDHeartbeatResponse* response) {
  (void)context;
  std::lock_guard<std::mutex> lock(nodes_mutex_);

  auto it = node_heartbeats_.find(request->node_id());
  if (it != node_heartbeats_.end()) {
    it->second.last_status.set_node_id(request->node_id());
    it->second.last_status.set_cpu_usage_percent(request->cpu_usage_percent());
    it->second.last_status.set_memory_usage_percent(request->memory_usage_percent());
    it->second.last_update = std::chrono::steady_clock::now();
    it->second.is_online = true;
  }

  response->set_success(true);
  return grpc::Status::OK;
}

grpc::Status MetaServiceHandler::GetSchema(grpc::ServerContext* context,
                                           const cedar::meta::GetSchemaRequest* request,
                                           cedar::meta::GetSchemaResponse* response) {
  (void)context;
  std::shared_lock<std::shared_mutex> lock(schema_mutex_);

  auto it = schema_cache_.find(request->space_name());
  if (it != schema_cache_.end()) {
    response->mutable_schema()->CopyFrom(it->second);
    response->set_success(true);
    return grpc::Status::OK;
  }

  // If no schema exists yet, return a default bootstrap schema
  cedar::meta::GraphSchema default_schema;

  cedar::meta::LabelSchema* person = (*default_schema.mutable_node_labels())["Person"].New();
  person->set_name("Person");
  person->set_is_node(true);
  auto* p = person->add_properties();
  p->set_name("id");
  p->set_type("STRING");
  p->set_nullable(false);
  p->set_indexed(true);
  auto* p2 = person->add_properties();
  p2->set_name("name");
  p2->set_type("STRING");
  (*default_schema.mutable_node_labels())["Person"] = *person;

  cedar::meta::LabelSchema* company = (*default_schema.mutable_node_labels())["Company"].New();
  company->set_name("Company");
  company->set_is_node(true);
  auto* c1 = company->add_properties();
  c1->set_name("id");
  c1->set_type("STRING");
  c1->set_nullable(false);
  c1->set_indexed(true);
  auto* c2 = company->add_properties();
  c2->set_name("name");
  c2->set_type("STRING");
  (*default_schema.mutable_node_labels())["Company"] = *company;

  cedar::meta::LabelSchema* works_for = (*default_schema.mutable_edge_types())["WORKS_FOR"].New();
  works_for->set_name("WORKS_FOR");
  works_for->set_is_node(false);
  auto* e1 = works_for->add_properties();
  e1->set_name("since");
  e1->set_type("STRING");
  (*default_schema.mutable_edge_types())["WORKS_FOR"] = *works_for;

  schema_cache_[request->space_name()] = default_schema;
  response->mutable_schema()->CopyFrom(default_schema);
  response->set_success(true);
  return grpc::Status::OK;
}

// ========== 私有方法 ==========

void MetaServiceHandler::HeartbeatCheckLoop() {
  while (running_) {
    std::this_thread::sleep_for(heartbeat_check_interval_);
    
    if (!running_) break;
    
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    auto now = std::chrono::steady_clock::now();
    
    for (auto& [node_id, record] : node_heartbeats_) {
      if (record.is_online && (now - record.last_update) > heartbeat_timeout_) {
        record.is_online = false;
        std::cout << "[MetaD] Node " << node_id << " marked as offline (heartbeat timeout)" << std::endl;
        
        // 触发分区重新平衡
        // TODO: 异步执行以避免阻塞心跳线程
      }
    }
  }
}

bool MetaServiceHandler::IsNodeExpiredLocked(const std::string& node_id) {
  (void)node_id;
  // TODO: 实现
  return false;
}

NodeInfo MetaServiceHandler::BuildNodeInfoLocked(uint32_t node_id) {
  NodeInfo info;
  info.set_node_id(node_id);
  
  auto it = node_heartbeats_.find(node_id);
  if (it != node_heartbeats_.end()) {
    const auto& record = it->second;
    info.set_address(record.last_status.node_id() ? "" : "");  // TODO: 从注册信息获取地址
    info.set_state(record.is_online ? "ONLINE" : "OFFLINE");
    info.set_last_heartbeat_unix(
        std::chrono::duration_cast<std::chrono::seconds>(
            record.last_update.time_since_epoch()).count());
    
    // 添加分区统计 (NodeInfo 没有 leader_count 字段，使用 data_path 存储)
    auto stats = partition_allocator_->GetStats();
    info.set_data_path("partitions=" + std::to_string(static_cast<int>(stats.avg_partitions_per_node)));
  }
  
  return info;
}

}  // namespace service
}  // namespace cedar
