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

#include "cedar/dtx/storage_service_impl.h"
#include "cedar/dtx/storage/partition_raft_manager.h"
#include "cedar/dtx/monitoring.h"
#include "cedar/storage/lsm_engine.h"

#include <cstring>
#include <iostream>

namespace cedar {
namespace dtx {

// =============================================================================
// StorageServiceImpl Implementation
// =============================================================================

StorageServiceImpl::StorageServiceImpl(StoragePartitionManager* partition_manager,
                                               PartitionRaftManager* raft_manager)
    : partition_manager_(partition_manager), raft_manager_(raft_manager) {
  if (partition_manager_) {
    storage_interface_ = std::make_unique<cedar::storage::StorageInterface>(
        partition_manager_->GetSharedStorage());
    cypher_engine_ = std::make_unique<cedar::cypher::CypherEngine>(
        partition_manager_->GetSharedStorage());
  }
}

StorageServiceImpl::~StorageServiceImpl() = default;

// Helper: Convert proto CedarKey to native CedarKey
CedarKey StorageServiceImpl::ProtoToCedarKey(const cedar::storage::CedarKey& proto_key) {
  // CedarKey constructor: entity_id, entity_type, column_id, timestamp, sequence, target_id, flags, part_id
  return CedarKey(
      proto_key.entity_id(),
      static_cast<EntityType>(proto_key.type_flags()),  // entity_type from type_flags
      static_cast<uint16_t>(proto_key.column_id()),
      Timestamp(proto_key.timestamp()),
      static_cast<uint16_t>(proto_key.sequence()),
      proto_key.target_id(),
      0,  // flags
      static_cast<PartitionID>(proto_key.partition_id()));
}

// Helper: Convert native CedarKey to proto CedarKey
cedar::storage::CedarKey StorageServiceImpl::CedarKeyToProto(const CedarKey& key) {
  cedar::storage::CedarKey proto;
  proto.set_entity_id(key.entity_id());
  proto.set_timestamp(key.timestamp().value());
  proto.set_target_id(key.target_id());
  proto.set_column_id(key.column_id());
  proto.set_sequence(key.sequence());
  proto.set_type_flags(key.flags());
  proto.set_partition_id(key.part_id());
  return proto;
}

namespace {

cedar::cypher::Value DeserializeCypherValue(const std::string& data) {
  if (data.empty()) return cedar::cypher::Value();
  char type_tag = data[0];
  switch (type_tag) {
    case 1:
      if (data.size() >= 2) {
        return cedar::cypher::Value(data[1] != 0);
      }
      break;
    case 2:
      if (data.size() >= 1 + sizeof(int64_t)) {
        int64_t v;
        memcpy(&v, data.data() + 1, sizeof(v));
        return cedar::cypher::Value(v);
      }
      break;
    case 3:
      if (data.size() >= 1 + sizeof(double)) {
        double v;
        memcpy(&v, data.data() + 1, sizeof(v));
        return cedar::cypher::Value(v);
      }
      break;
    case 4:
      if (data.size() >= 1 + sizeof(uint32_t)) {
        uint32_t len;
        memcpy(&len, data.data() + 1, sizeof(len));
        if (data.size() >= 1 + sizeof(uint32_t) + len) {
          return cedar::cypher::Value(
              std::string(data.data() + 1 + sizeof(uint32_t), len));
        }
      }
      break;
  }
  return cedar::cypher::Value();
}

std::string SerializeDescriptorSimple(const cedar::Descriptor& desc) {
  std::string data;
  if (desc.GetKind() == cedar::EntryKind::InlineInt) {
    auto val = desc.AsInlineInt();
    if (val.has_value()) {
      int32_t value = val.value();
      data.assign(reinterpret_cast<const char*>(&value), sizeof(value));
    }
  } else if (desc.GetKind() == cedar::EntryKind::InlineShortStr) {
    data = desc.AsInlineShortStr();
  } else {
    uint64_t raw = desc.AsRaw();
    data.assign(reinterpret_cast<const char*>(&raw), sizeof(raw));
  }
  return data;
}

}  // namespace

std::vector<cedar::storage::PropertyPredicateItem> StorageServiceImpl::ConvertPredicates(
    const google::protobuf::RepeatedPtrField<cedar::storage::ScanPredicate>& proto_preds) {
  std::vector<cedar::storage::PropertyPredicateItem> result;
  for (const auto& p : proto_preds) {
    if (p.has_property()) {
      cedar::storage::PropertyPredicateItem item;
      item.property_name = p.property().property_name();
      switch (p.property().op()) {
        case cedar::storage::EQ: item.op = cedar::storage::PropertyPredicateItem::EQ; break;
        case cedar::storage::NE: item.op = cedar::storage::PropertyPredicateItem::NE; break;
        case cedar::storage::LT: item.op = cedar::storage::PropertyPredicateItem::LT; break;
        case cedar::storage::LE: item.op = cedar::storage::PropertyPredicateItem::LE; break;
        case cedar::storage::GT: item.op = cedar::storage::PropertyPredicateItem::GT; break;
        case cedar::storage::GE: item.op = cedar::storage::PropertyPredicateItem::GE; break;
        case cedar::storage::IN: item.op = cedar::storage::PropertyPredicateItem::IN; break;
        default: item.op = cedar::storage::PropertyPredicateItem::EQ; break;
      }
      if (!p.property().serialized_value().empty()) {
        item.value = DeserializeCypherValue(p.property().serialized_value());
      }
      result.push_back(std::move(item));
    }
  }
  return result;
}

// Helper: Convert proto Descriptor to native Descriptor
// Supports both 8-byte raw Descriptor values and 4-byte InlineInt payloads.
// For 4-byte payloads, the column_id from the key is injected.
Descriptor StorageServiceImpl::ProtoToDescriptor(const cedar::storage::Descriptor& proto_desc,
                                                  uint16_t column_id) {
  const std::string& data = proto_desc.data();
  if (data.size() >= sizeof(uint64_t)) {
    uint64_t raw_value;
    std::memcpy(&raw_value, data.data(), sizeof(uint64_t));
    return Descriptor(raw_value);
  }
  if (data.size() >= sizeof(int32_t)) {
    int32_t value;
    std::memcpy(&value, data.data(), sizeof(int32_t));
    return Descriptor(EntryKind::InlineInt, column_id,
                      static_cast<uint32_t>(value), sizeof(int32_t));
  }
  return Descriptor();  // Return empty descriptor if no data
}

// Helper: Convert native Descriptor to proto Descriptor
// Serializes the payload in a format compatible with ProtoToDescriptor.
cedar::storage::Descriptor StorageServiceImpl::DescriptorToProto(const Descriptor& desc) {
  cedar::storage::Descriptor proto;
  if (desc.GetKind() == EntryKind::InlineInt) {
    auto val = desc.AsInlineInt();
    if (val.has_value()) {
      int32_t value = val.value();
      proto.set_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
  } else if (desc.GetKind() == EntryKind::InlineShortStr) {
    proto.set_data(desc.AsInlineShortStr());
  } else if (desc.GetKind() == EntryKind::InlineFloat) {
    auto val = desc.AsInlineFloat();
    if (val.has_value()) {
      union { float f; uint32_t i; } u;
      u.f = val.value();
      proto.set_data(reinterpret_cast<const char*>(&u.i), sizeof(u.i));
    }
  } else {
    // Fallback: send raw 8-byte descriptor for other kinds
    uint64_t raw_value = desc.AsRaw();
    proto.set_data(reinterpret_cast<const char*>(&raw_value), sizeof(raw_value));
  }
  return proto;
}

// =============================================================================
// Basic Operations
// =============================================================================

grpc::Status StorageServiceImpl::Put(grpc::ServerContext* context,
                                      const cedar::storage::PutRequest* request,
                                      cedar::storage::PutResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  PartitionID pid = static_cast<PartitionID>(request->key().partition_id());
  auto* partition = partition_manager_->GetPartition(pid);
  
  if (!partition) {
    response->set_success(false);
    response->set_error_msg("Partition not found: " + std::to_string(pid));
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Partition not found");
  }
  
  // Convert proto key to CedarKey
  CedarKey key = ProtoToCedarKey(request->key());
  
  // Convert descriptor (inject column_id from key for InlineInt compatibility)
  Descriptor desc = ProtoToDescriptor(request->descriptor_(), key.column_id());
  
  // Convert timestamp and txn_id
  Timestamp txn_version(request->txn_version().value());
  TxnID txn_id = request->txn_id();
  
  // If braft replication is enabled, propose through Raft
  if (raft_manager_) {
    auto* raft_group = raft_manager_->GetRaftGroup(pid);
    if (!raft_group) {
      response->set_success(false);
      response->set_error_msg("Raft group not found for partition: " + std::to_string(pid));
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Raft group not found");
    }
    if (!raft_group->IsLeader()) {
      auto leader_id = raft_group->GetLeaderId();
      auto leader_addr = raft_group->GetLeaderAddress();
      response->set_success(false);
      if (leader_id.has_value() && leader_addr.has_value()) {
        response->set_error_msg("Not leader, redirect to node " +
                                std::to_string(leader_id.value()) + " at " + leader_addr.value());
      } else if (leader_addr.has_value()) {
        response->set_error_msg("Not leader, redirect to " + leader_addr.value());
      } else {
        response->set_error_msg("Not leader, leader unknown");
      }
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader");
    }
    StorageLogEntry entry;
    entry.type = StorageLogEntry::Type::kPut;
    entry.key = key;
    entry.descriptor = desc;
    entry.txn_version = txn_version;
    auto status = raft_group->Propose(entry);
    if (status.ok()) {
      response->set_success(true);
      return grpc::Status::OK;
    } else {
      response->set_success(false);
      response->set_error_msg(status.ToString());
      return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
    }
  }

  // Direct write path (no replication or single-node mode)
  auto status = partition->Put(key, desc, txn_version, txn_id);
  
  if (status.ok()) {
    response->set_success(true);
    return grpc::Status::OK;
  } else {
    response->set_success(false);
    response->set_error_msg(status.ToString());
    return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
  }
}

// Helper: check if this node is the leader for the partition.
// If not leader, returns UNAVAILABLE with leader address hint.
grpc::Status StorageServiceImpl::CheckReadLeader(PartitionID pid, std::string* leader_hint) {
  if (!raft_manager_) {
    return grpc::Status::OK;  // No raft = single node, always allow
  }
  auto* raft_group = raft_manager_->GetRaftGroup(pid);
  if (!raft_group) {
    return grpc::Status::OK;  // No raft group for this partition yet
  }
  if (raft_group->IsLeader() && raft_group->IsLeaseValid()) {
    return grpc::Status::OK;
  }
  auto leader_addr = raft_group->GetLeaderAddress();
  if (leader_addr.has_value() && leader_hint) {
    *leader_hint = leader_addr.value();
  }
  return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader or lease expired");
}

grpc::Status StorageServiceImpl::Get(grpc::ServerContext* context,
                                      const cedar::storage::GetRequest* request,
                                      cedar::storage::GetResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  PartitionID pid = static_cast<PartitionID>(request->key().partition_id());
  auto* partition = partition_manager_->GetPartition(pid);
  
  if (!partition) {
    response->set_success(false);
    response->set_found(false);
    response->set_error_msg("Partition not found: " + std::to_string(pid));
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Partition not found");
  }

  // Leader check for linearizable read
  std::string leader_hint;
  auto leader_status = CheckReadLeader(pid, &leader_hint);
  if (!leader_status.ok()) {
    response->set_success(false);
    response->set_error_msg("Not leader, redirect to: " + leader_hint);
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Not leader", leader_hint);
  }
  
  // Convert proto key to CedarKey
  CedarKey key = ProtoToCedarKey(request->key());
  
  // Use the timestamp from the request key; default to Max for latest value
  Timestamp read_time = key.timestamp().value() > 0 ? key.timestamp() : Timestamp::Max();
  
  // Execute get
  auto result = partition->Get(key, read_time);
  
  if (result.ok()) {
    response->set_success(true);
    response->set_found(true);
    *response->mutable_descriptor_() = DescriptorToProto(result.value());
    return grpc::Status::OK;
  } else if (result.status().IsNotFound()) {
    response->set_success(true);
    response->set_found(false);
    return grpc::Status::OK;
  } else {
    response->set_success(false);
    response->set_found(false);
    response->set_error_msg(result.status().ToString());
    return grpc::Status(grpc::StatusCode::INTERNAL, result.status().ToString());
  }
}

grpc::Status StorageServiceImpl::Delete(grpc::ServerContext* context,
                                         const cedar::storage::DeleteRequest* request,
                                         cedar::storage::DeleteResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  PartitionID pid = static_cast<PartitionID>(request->key().partition_id());
  auto* partition = partition_manager_->GetPartition(pid);
  
  if (!partition) {
    response->set_success(false);
    response->set_error_msg("Partition not found: " + std::to_string(pid));
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Partition not found");
  }
  
  // Convert proto key to CedarKey
  CedarKey key = ProtoToCedarKey(request->key());
  
  // Convert timestamp and txn_id
  Timestamp txn_version(request->txn_version().value());
  TxnID txn_id = request->txn_id();
  
  // If braft replication is enabled, propose through Raft
  if (raft_manager_) {
    auto* raft_group = raft_manager_->GetRaftGroup(pid);
    if (!raft_group) {
      response->set_success(false);
      response->set_error_msg("Raft group not found for partition: " + std::to_string(pid));
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Raft group not found");
    }
    if (!raft_group->IsLeader()) {
      auto leader_id = raft_group->GetLeaderId();
      auto leader_addr = raft_group->GetLeaderAddress();
      response->set_success(false);
      if (leader_id.has_value() && leader_addr.has_value()) {
        response->set_error_msg("Not leader, redirect to node " +
                                std::to_string(leader_id.value()) + " at " + leader_addr.value());
      } else if (leader_addr.has_value()) {
        response->set_error_msg("Not leader, redirect to " + leader_addr.value());
      } else {
        response->set_error_msg("Not leader, leader unknown");
      }
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader");
    }
    StorageLogEntry entry;
    entry.type = StorageLogEntry::Type::kDelete;
    entry.key = key;
    entry.txn_version = txn_version;
    auto status = raft_group->Propose(entry);
    if (status.ok()) {
      response->set_success(true);
      return grpc::Status::OK;
    } else {
      response->set_success(false);
      response->set_error_msg(status.ToString());
      return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
    }
  }

  // Direct write path (no replication or single-node mode)
  Descriptor empty_desc;
  auto status = partition->Put(key, empty_desc, txn_version, txn_id);
  
  if (status.ok()) {
    response->set_success(true);
    return grpc::Status::OK;
  } else {
    response->set_success(false);
    response->set_error_msg(status.ToString());
    return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
  }
}

grpc::Status StorageServiceImpl::Scan(grpc::ServerContext* context,
                                       const cedar::storage::ScanRequest* request,
                                       cedar::storage::ScanResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  // Leader check for linearizable read
  PartitionID pid = static_cast<PartitionID>(request->partition_id());
  std::string leader_hint;
  auto leader_status = CheckReadLeader(pid, &leader_hint);
  if (!leader_status.ok()) {
    response->set_success(false);
    response->set_error_msg("Not leader, redirect to: " + leader_hint);
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Not leader", leader_hint);
  }

  if (!storage_interface_) {
    response->set_success(false);
    response->set_error_msg("Storage interface not initialized");
    return grpc::Status::OK;
  }

  std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>> results;
  Status s = storage_interface_->ScanVertices(
      request->entity_id(),
      cedar::Timestamp(request->start_time()),
      cedar::Timestamp(request->end_time()),
      {},  // no predicates for legacy Scan
      &results);

  if (!s.ok()) {
    response->set_success(false);
    response->set_error_msg(s.ToString());
    return grpc::Status::OK;
  }

  for (const auto& [ts, desc] : results) {
    auto* item = response->add_items();
    item->set_timestamp(ts.value());
    item->mutable_descriptor_()->set_data(SerializeDescriptorSimple(desc));
  }

  response->set_success(true);
  return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::ScanNodeV2(grpc::ServerContext* context,
                                             const cedar::storage::ScanNodeRequestV2* request,
                                             cedar::storage::ScanResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  // Leader check for linearizable read
  PartitionID pid = static_cast<PartitionID>(request->partition_id());
  std::string leader_hint;
  auto leader_status = CheckReadLeader(pid, &leader_hint);
  if (!leader_status.ok()) {
    response->set_success(false);
    response->set_error_msg("Not leader, redirect to: " + leader_hint);
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Not leader", leader_hint);
  }

  if (!storage_interface_) {
    response->set_success(false);
    response->set_error_msg("Storage interface not initialized");
    return grpc::Status::OK;
  }

  std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>> results;
  auto predicates = ConvertPredicates(request->predicates());
  Status s = storage_interface_->ScanVertices(
      request->node_id(),
      cedar::Timestamp(request->start_time()),
      cedar::Timestamp(request->end_time()),
      predicates,
      &results);

  if (!s.ok()) {
    response->set_success(false);
    response->set_error_msg(s.ToString());
    return grpc::Status::OK;
  }

  for (const auto& [ts, desc] : results) {
    auto* item = response->add_items();
    item->set_timestamp(ts.value());
    item->mutable_descriptor_()->set_data(SerializeDescriptorSimple(desc));
  }

  response->set_success(true);
  return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::ScanEdgeV2(grpc::ServerContext* context,
                                             const cedar::storage::ScanEdgeRequestV2* request,
                                             cedar::storage::ScanResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  // Leader check for linearizable read
  PartitionID pid = static_cast<PartitionID>(request->partition_id());
  std::string leader_hint;
  auto leader_status = CheckReadLeader(pid, &leader_hint);
  if (!leader_status.ok()) {
    response->set_success(false);
    response->set_error_msg("Not leader, redirect to: " + leader_hint);
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Not leader", leader_hint);
  }

  if (!storage_interface_) {
    response->set_success(false);
    response->set_error_msg("Storage interface not initialized");
    return grpc::Status::OK;
  }

  std::vector<std::pair<cedar::Timestamp, cedar::Descriptor>> results;
  auto predicates = ConvertPredicates(request->predicates());

  if (request->direction() == cedar::storage::Direction::OUTGOING ||
      request->direction() == cedar::storage::Direction::BOTH) {
    Status s = storage_interface_->ScanOutEdges(
        request->node_id(),
        static_cast<uint16_t>(request->edge_type()),
        cedar::Timestamp(request->start_time()),
        cedar::Timestamp(request->end_time()),
        predicates,
        &results);
    if (!s.ok()) {
      response->set_success(false);
      response->set_error_msg(s.ToString());
      return grpc::Status::OK;
    }
  }

  if (request->direction() == cedar::storage::Direction::INCOMING ||
      request->direction() == cedar::storage::Direction::BOTH) {
    Status s = storage_interface_->ScanInEdges(
        request->node_id(),
        static_cast<uint16_t>(request->edge_type()),
        cedar::Timestamp(request->start_time()),
        cedar::Timestamp(request->end_time()),
        predicates,
        &results);
    if (!s.ok()) {
      response->set_success(false);
      response->set_error_msg(s.ToString());
      return grpc::Status::OK;
    }
  }

  for (const auto& [ts, desc] : results) {
    auto* item = response->add_items();
    item->set_timestamp(ts.value());
    item->mutable_descriptor_()->set_data(SerializeDescriptorSimple(desc));
  }

  response->set_success(true);
  return grpc::Status::OK;
}

// =============================================================================
// Batch Operations
// =============================================================================

grpc::Status StorageServiceImpl::BatchPut(grpc::ServerContext* context,
                                           const cedar::storage::BatchPutRequest* request,
                                           cedar::storage::BatchPutResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  Timestamp txn_version(request->txn_version().value());
  TxnID txn_id = request->txn_id();
  
  bool all_success = true;

  // If braft replication is enabled, propose through Raft
  if (raft_manager_) {
    for (const auto& item : request->items()) {
      PartitionID pid = static_cast<PartitionID>(item.key().partition_id());
      auto* raft_group = raft_manager_->GetRaftGroup(pid);
      if (!raft_group) {
        response->add_item_success(false);
        all_success = false;
        continue;
      }
      if (!raft_group->IsLeader()) {
        response->add_item_success(false);
        all_success = false;
        auto leader_id = raft_group->GetLeaderId();
        auto leader_addr = raft_group->GetLeaderAddress();
        if (leader_id.has_value() && leader_addr.has_value()) {
          response->set_error_msg("Not leader for partition " + std::to_string(pid) +
                                  ", redirect to node " + std::to_string(leader_id.value()) +
                                  " at " + leader_addr.value());
        } else if (leader_addr.has_value()) {
          response->set_error_msg("Not leader for partition " + std::to_string(pid) +
                                  ", redirect to " + leader_addr.value());
        } else {
          response->set_error_msg("Not leader for partition " + std::to_string(pid) +
                                  ", leader unknown");
        }
        continue;
      }
      CedarKey key = ProtoToCedarKey(item.key());
      Descriptor desc = ProtoToDescriptor(item.descriptor_(), key.column_id());
      StorageLogEntry entry;
      entry.type = StorageLogEntry::Type::kPut;
      entry.key = key;
      entry.descriptor = desc;
      entry.txn_version = txn_version;
      auto status = raft_group->Propose(entry);
      response->add_item_success(status.ok());
      if (!status.ok()) {
        all_success = false;
      }
    }
    response->set_success(all_success);
    if (!all_success) {
      response->set_error_msg("Some items failed to propose");
    }
    return grpc::Status::OK;
  }

  // Direct write path (no replication or single-node mode)
  for (const auto& item : request->items()) {
    PartitionID pid = static_cast<PartitionID>(item.key().partition_id());
    auto* partition = partition_manager_->GetPartition(pid);
    
    if (!partition) {
      response->add_item_success(false);
      all_success = false;
      continue;
    }
    
    CedarKey key = ProtoToCedarKey(item.key());
    Descriptor desc = ProtoToDescriptor(item.descriptor_(), key.column_id());
    
    auto status = partition->Put(key, desc, txn_version, txn_id);
    response->add_item_success(status.ok());
    if (!status.ok()) {
      all_success = false;
    }
  }
  
  response->set_success(all_success);
  if (!all_success) {
    response->set_error_msg("Some items failed to write");
  }
  
  return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::BatchGet(grpc::ServerContext* context,
                                           const cedar::storage::BatchGetRequest* request,
                                           cedar::storage::BatchGetResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  // Leader check: all keys must belong to partitions we lead
  // For simplicity, check the first key's partition (typical use case)
  if (!request->keys().empty()) {
    PartitionID first_pid = static_cast<PartitionID>(request->keys(0).partition_id());
    std::string leader_hint;
    auto leader_status = CheckReadLeader(first_pid, &leader_hint);
    if (!leader_status.ok()) {
      response->set_success(false);
      response->set_error_msg("Not leader, redirect to: " + leader_hint);
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                          "Not leader", leader_hint);
    }
  }
  
  // For BatchGet, we use max timestamp to get latest values
  Timestamp read_time = Timestamp::Max();
  
  for (const auto& proto_key : request->keys()) {
    PartitionID pid = static_cast<PartitionID>(proto_key.partition_id());
    auto* partition = partition_manager_->GetPartition(pid);
    
    if (!partition) {
      response->add_descriptors();
      response->add_found(false);
      continue;
    }
    
    CedarKey key = ProtoToCedarKey(proto_key);
    auto result = partition->Get(key, read_time);
    
    if (result.ok()) {
      *response->add_descriptors() = DescriptorToProto(result.value());
      response->add_found(true);
    } else {
      response->add_descriptors();
      response->add_found(false);
    }
  }
  
  response->set_success(true);
  return grpc::Status::OK;
}

// =============================================================================
// 2PC Distributed Transaction Support
// =============================================================================

grpc::Status StorageServiceImpl::Prepare(grpc::ServerContext* context,
                                          const cedar::storage::PrepareRequest* request,
                                          cedar::storage::PrepareResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  TxnID txn_id = request->txn_id();
  Timestamp commit_ts(request->commit_ts());
  
  // Convert read/write sets
  std::vector<CedarKey> read_set;
  std::vector<CedarKey> write_set;
  
  for (const auto& proto_key : request->read_set()) {
    read_set.push_back(ProtoToCedarKey(proto_key));
  }
  
  for (const auto& proto_key : request->write_set()) {
    write_set.push_back(ProtoToCedarKey(proto_key));
  }
  
  // Convert write_descriptors
  std::unordered_map<uint64_t, Descriptor> write_descriptors;
  for (const auto& [key_hash, proto_desc] : request->write_descriptors()) {
    // Proto Descriptor has bytes data, convert to native Descriptor
    if (!proto_desc.data().empty() && proto_desc.data().size() == sizeof(uint64_t)) {
      uint64_t raw;
      std::memcpy(&raw, proto_desc.data().data(), sizeof(raw));
      write_descriptors[key_hash] = Descriptor(raw);
    }
  }
  
  // Collect all involved partitions from read_set and write_set
  std::set<PartitionID> involved_partitions;
  for (const auto& key : write_set) {
    involved_partitions.insert(key.part_id());
  }
  for (const auto& key : read_set) {
    involved_partitions.insert(key.part_id());
  }
  
  if (involved_partitions.empty()) {
    response->set_prepared(false);
    response->set_error_msg("No partitions involved");
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "No partitions involved");
  }
  
  bool all_prepared = true;
  std::string last_error;
  std::vector<PartitionID> prepared_partitions;
  
  for (PartitionID pid : involved_partitions) {
    auto* partition = partition_manager_->GetPartition(pid);
    if (!partition) {
      all_prepared = false;
      last_error = "Partition not found: " + std::to_string(pid);
      break;
    }
    
    // If braft replication is enabled, propose through Raft
    if (raft_manager_) {
      auto* raft_group = raft_manager_->GetRaftGroup(pid);
      if (!raft_group) {
        all_prepared = false;
        last_error = "Raft group not found for partition: " + std::to_string(pid);
        break;
      }
      if (!raft_group->IsLeader()) {
        auto leader_id = raft_group->GetLeaderId();
        auto leader_addr = raft_group->GetLeaderAddress();
        response->set_prepared(false);
        if (leader_id.has_value() && leader_addr.has_value()) {
          response->set_error_msg("Not leader for partition " + std::to_string(pid) +
                                  ", redirect to node " + std::to_string(leader_id.value()) +
                                  " at " + leader_addr.value());
        } else {
          response->set_error_msg("Not leader for partition " + std::to_string(pid));
        }
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader");
      }
      StorageLogEntry entry;
      entry.type = StorageLogEntry::Type::kPrepare;
      entry.txn_id = txn_id;
      entry.read_set = read_set;
      entry.write_set = write_set;
      entry.write_descriptors = write_descriptors;
      entry.commit_ts = commit_ts;
      auto status = raft_group->Propose(entry);
      if (!status.ok()) {
        all_prepared = false;
        last_error = "Raft propose failed: " + status.ToString();
        break;
      }
      prepared_partitions.push_back(pid);
      continue;
    }
    
    // Direct path (no replication or single-node mode)
    auto status = partition->Prepare(txn_id, read_set, write_set, write_descriptors, commit_ts);
    if (!status.ok()) {
      all_prepared = false;
      last_error = status.ToString();
      break;
    }
    prepared_partitions.push_back(pid);
  }
  
  if (!all_prepared) {
    // Rollback already prepared partitions to avoid hanging prepared states
    for (PartitionID pid : prepared_partitions) {
      auto* partition = partition_manager_->GetPartition(pid);
      if (partition) {
        partition->Abort(txn_id);  // Best-effort rollback
      }
    }
    response->set_prepared(false);
    response->set_error_msg(last_error);
    return grpc::Status::OK;
  }
  
  {
    std::lock_guard<std::mutex> lock(txn_partitions_mutex_);
    txn_partitions_[txn_id] = std::move(involved_partitions);
  }
  response->set_prepared(true);
  response->set_prepared_ts(commit_ts.value());
  return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::Commit(grpc::ServerContext* context,
                                         const cedar::storage::CommitRequest* request,
                                         cedar::storage::CommitResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  try {
    TxnID txn_id = request->txn_id();
    Timestamp commit_ts(request->commit_ts());
    
    bool all_committed = true;
    std::vector<std::string> errors;
    
    std::set<PartitionID> involved_partitions;
    {
      std::lock_guard<std::mutex> lock(txn_partitions_mutex_);
      auto it = txn_partitions_.find(txn_id);
      if (it != txn_partitions_.end()) {
        involved_partitions = it->second;
      }
    }
    
    if (involved_partitions.empty()) {
      // Try to rebuild from prepared_txns_ after snapshot load or leader change
      RebuildTxnPartitions();
      {
        std::lock_guard<std::mutex> lock(txn_partitions_mutex_);
        auto it = txn_partitions_.find(txn_id);
        if (it != txn_partitions_.end()) {
          involved_partitions = it->second;
        }
      }
    }
    
    if (involved_partitions.empty()) {
      // Fallback to all partitions if mapping missing
      involved_partitions.insert(partition_manager_->GetAllPartitions().begin(),
                                 partition_manager_->GetAllPartitions().end());
    }
    
    for (PartitionID pid : involved_partitions) {
      auto* partition = partition_manager_->GetPartition(pid);
      if (!partition) {
        all_committed = false;
        errors.push_back("Partition not found: " + std::to_string(pid));
        continue;  // Try remaining partitions
      }
      
      // If braft replication is enabled, propose through Raft
      if (raft_manager_) {
        auto* raft_group = raft_manager_->GetRaftGroup(pid);
        if (!raft_group) {
          all_committed = false;
          errors.push_back("Raft group not found for partition: " + std::to_string(pid));
          continue;
        }
        if (!raft_group->IsLeader()) {
          auto leader_id = raft_group->GetLeaderId();
          auto leader_addr = raft_group->GetLeaderAddress();
          response->set_success(false);
          if (leader_id.has_value() && leader_addr.has_value()) {
            response->set_error_msg("Not leader for partition " + std::to_string(pid) +
                                    ", redirect to node " + std::to_string(leader_id.value()) +
                                    " at " + leader_addr.value());
          } else {
            response->set_error_msg("Not leader for partition " + std::to_string(pid));
          }
          return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader");
        }
        StorageLogEntry entry;
        entry.type = StorageLogEntry::Type::kCommit;
        entry.txn_id = txn_id;
        entry.commit_ts = commit_ts;
        auto status = raft_group->Propose(entry);
        if (!status.ok()) {
          all_committed = false;
          errors.push_back("Raft propose failed for partition " + std::to_string(pid) +
                           ": " + status.ToString());
          continue;
        }
        continue;
      }
      
      // Direct path (no replication or single-node mode)
      auto status = partition->Commit(txn_id, commit_ts);
      if (!status.ok()) {
        all_committed = false;
        errors.push_back("Partition " + std::to_string(pid) + ": " + status.ToString());
        continue;  // Try remaining partitions
      }
    }
    
    if (all_committed) {
      response->set_success(true);
      return grpc::Status::OK;
    } else {
      response->set_success(false);
      std::string error_msg = "Commit failed on one or more partitions: ";
      for (const auto& e : errors) {
        error_msg += e + "; ";
      }
      response->set_error_msg(error_msg);
      return grpc::Status(grpc::StatusCode::INTERNAL, error_msg);
    }
  } catch (const std::exception& e) {
    std::cerr << "[StorageServiceImpl::Commit] Exception: " << e.what() << std::endl;
    response->set_success(false);
    response->set_error_msg(std::string("Exception: ") + e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  } catch (...) {
    std::cerr << "[StorageServiceImpl::Commit] Unknown exception" << std::endl;
    response->set_success(false);
    response->set_error_msg("Unknown exception");
    return grpc::Status(grpc::StatusCode::INTERNAL, "Unknown exception");
  }
}

void StorageServiceImpl::RebuildTxnPartitions() {
  std::lock_guard<std::mutex> lock(txn_partitions_mutex_);
  txn_partitions_.clear();
  auto all_partitions = partition_manager_->GetAllPartitions();
  for (PartitionID pid : all_partitions) {
    auto* partition = partition_manager_->GetPartition(pid);
    if (!partition) continue;
    auto prepared_txns = partition->GetPreparedTransactions();
    for (TxnID txn_id : prepared_txns) {
      txn_partitions_[txn_id].insert(pid);
    }
  }
}

grpc::Status StorageServiceImpl::Abort(grpc::ServerContext* context,
                                        const cedar::storage::AbortRequest* request,
                                        cedar::storage::AbortResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  TxnID txn_id = request->txn_id();
  
  bool any_success = false;
  std::string last_error;
  
  std::set<PartitionID> involved_partitions;
  {
    std::lock_guard<std::mutex> lock(txn_partitions_mutex_);
    auto it = txn_partitions_.find(txn_id);
    if (it != txn_partitions_.end()) {
      involved_partitions = it->second;
    }
  }
  
  if (involved_partitions.empty()) {
    RebuildTxnPartitions();
    {
      std::lock_guard<std::mutex> lock(txn_partitions_mutex_);
      auto it = txn_partitions_.find(txn_id);
      if (it != txn_partitions_.end()) {
        involved_partitions = it->second;
      }
    }
  }
  
  if (involved_partitions.empty()) {
    involved_partitions.insert(partition_manager_->GetAllPartitions().begin(),
                               partition_manager_->GetAllPartitions().end());
  }
  
  for (PartitionID pid : involved_partitions) {
    auto* partition = partition_manager_->GetPartition(pid);
    if (!partition) {
      last_error = "Partition not found: " + std::to_string(pid);
      continue;
    }
    
    // If braft replication is enabled, propose through Raft
    if (raft_manager_) {
      auto* raft_group = raft_manager_->GetRaftGroup(pid);
      if (!raft_group) {
        last_error = "Raft group not found for partition: " + std::to_string(pid);
        continue;
      }
      if (!raft_group->IsLeader()) {
        auto leader_id = raft_group->GetLeaderId();
        auto leader_addr = raft_group->GetLeaderAddress();
        response->set_success(false);
        if (leader_id.has_value() && leader_addr.has_value()) {
          response->set_error_msg("Not leader for partition " + std::to_string(pid) +
                                  ", redirect to node " + std::to_string(leader_id.value()) +
                                  " at " + leader_addr.value());
        } else {
          response->set_error_msg("Not leader for partition " + std::to_string(pid));
        }
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader");
      }
      StorageLogEntry entry;
      entry.type = StorageLogEntry::Type::kAbort;
      entry.txn_id = txn_id;
      auto status = raft_group->Propose(entry);
      if (status.ok()) {
        any_success = true;
      } else {
        last_error = "Raft propose failed: " + status.ToString();
      }
      continue;
    }
    
    // Direct path (no replication or single-node mode)
    auto status = partition->Abort(txn_id);
    if (status.ok()) {
      any_success = true;
    } else {
      last_error = status.ToString();
    }
  }
  
  if (any_success) {
    response->set_success(true);
    return grpc::Status::OK;
  } else {
    response->set_success(false);
    response->set_error_msg(last_error.empty() ? "No partitions found" : last_error);
    return grpc::Status(grpc::StatusCode::INTERNAL, last_error);
  }
}

grpc::Status StorageServiceImpl::Inquire(grpc::ServerContext* context,
                                          const cedar::storage::InquireRequest* request,
                                          cedar::storage::InquireResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  TxnID txn_id = request->txn_id();
  response->set_txn_id(txn_id);
  
  // Query all partitions for this transaction's state
  bool found = false;
  bool found_committed = false;
  bool found_aborted = false;
  bool found_prepared = false;
  
  std::set<PartitionID> involved_partitions;
  {
    std::lock_guard<std::mutex> lock(txn_partitions_mutex_);
    auto it = txn_partitions_.find(txn_id);
    if (it != txn_partitions_.end()) {
      involved_partitions = it->second;
    }
  }
  
  if (involved_partitions.empty()) {
    RebuildTxnPartitions();
    {
      std::lock_guard<std::mutex> lock(txn_partitions_mutex_);
      auto it = txn_partitions_.find(txn_id);
      if (it != txn_partitions_.end()) {
        involved_partitions = it->second;
      }
    }
  }
  
  if (involved_partitions.empty()) {
    // Fallback: check all partitions
    involved_partitions.insert(partition_manager_->GetAllPartitions().begin(),
                               partition_manager_->GetAllPartitions().end());
  }
  
  for (PartitionID pid : involved_partitions) {
    auto* partition = partition_manager_->GetPartition(pid);
    if (!partition) continue;
    
    DistributedTxnState partition_state;
    auto status = partition->Inquire(txn_id, &partition_state);
    if (status.ok()) {
      found = true;
      if (partition_state == DistributedTxnState::kCommitted) {
        found_committed = true;
      } else if (partition_state == DistributedTxnState::kAborted) {
        found_aborted = true;
      } else if (partition_state == DistributedTxnState::kPrepared) {
        found_prepared = true;
      }
    }
  }
  
  if (!found) {
    response->set_state(cedar::storage::InquireResponse::UNKNOWN);
    response->set_error_msg("Transaction not found on this node");
    return grpc::Status::OK;
  }
  
  // Detect inconsistency: both committed and aborted seen across partitions
  if (found_committed && found_aborted) {
    response->set_state(cedar::storage::InquireResponse::INCONSISTENT);
    response->set_error_msg("Inconsistent state detected: transaction found both committed and aborted across partitions");
    return grpc::Status::OK;
  }
  
  if (found_committed) {
    response->set_state(cedar::storage::InquireResponse::COMMITTED);
  } else if (found_prepared) {
    response->set_state(cedar::storage::InquireResponse::PREPARED);
  } else if (found_aborted) {
    response->set_state(cedar::storage::InquireResponse::ABORTED);
  } else {
    response->set_state(cedar::storage::InquireResponse::UNKNOWN);
  }
  
  return grpc::Status::OK;
}

// =============================================================================
// GCN Compute-Optimized APIs
// =============================================================================

grpc::Status StorageServiceImpl::GetRangeForCompute(
    grpc::ServerContext* context,
    const cedar::storage::GetRangeForComputeRequest* request,
    cedar::storage::GetRangeForComputeResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  auto* storage = partition_manager_->GetSharedStorage();
  if (!storage) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "Storage not available");
  }

  auto* lsm_engine = storage->GetLsmEngine();
  if (!lsm_engine) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "LSM engine not available");
  }

  cedar::EntityType direction = (request->direction() == 0)
      ? cedar::EntityType::EdgeOut : cedar::EntityType::EdgeIn;

  auto results = lsm_engine->ScanEdgesWithFolding(
      request->entity_id(),
      direction,
      static_cast<uint16_t>(request->edge_type()),
      cedar::Timestamp(request->snapshot_ts()));

  constexpr size_t kMaxResults = 10000;
  bool truncated = false;
  if (results.size() > kMaxResults) {
    truncated = true;
    results.resize(kMaxResults);
  }

  for (const auto& entry : results) {
    auto* edge = response->add_edges();
    edge->set_target_id(entry.target_id);
    edge->set_valid_from(entry.timestamp.value());
    edge->set_valid_to(0);  // Still valid
    edge->set_edge_type(entry.edge_type);
  }

  LOG(WARNING) << "GetRangeForCompute served_version stub (entity="
               << request->entity_id() << ")";
  response->set_served_version(0);
  response->set_truncated(truncated);

  return grpc::Status::OK;
}

grpc::Status StorageServiceImpl::GetCommittedVersion(
    grpc::ServerContext* context,
    const cedar::storage::GetCommittedVersionRequest* request,
    cedar::storage::GetCommittedVersionResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  (void)request;

  LOG(WARNING) << "GetCommittedVersion stub called";
  response->set_committed_version(0);
  response->set_watermark(0);

  return grpc::Status::OK;
}

// =============================================================================
// Partition Management
// =============================================================================

grpc::Status StorageServiceImpl::GetPartitionInfo(grpc::ServerContext* context,
                                                   const cedar::storage::GetPartitionInfoRequest* request,
                                                   cedar::storage::GetPartitionInfoResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  PartitionID pid = static_cast<PartitionID>(request->partition_id());
  auto* partition = partition_manager_->GetPartition(pid);
  
  if (!partition) {
    response->set_success(false);
    response->set_error_msg("Partition not found: " + std::to_string(pid));
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Partition not found");
  }
  
  auto stats = partition->GetStats();
  
  // Check Raft leader status
  bool is_leader = true;
  std::string raft_role = "LEADER";
  if (raft_manager_) {
    auto* raft_group = raft_manager_->GetRaftGroup(pid);
    if (raft_group) {
      is_leader = raft_group->IsLeader();
      raft_role = is_leader ? "LEADER" : "FOLLOWER";
    }
  }
  
  auto* info = response->mutable_info();
  info->set_partition_id(pid);
  info->set_key_count(stats.num_keys);
  info->set_data_size(stats.disk_usage_bytes);
  info->set_qps(0);  // QPS tracking requires query counter instrumentation
  info->set_is_leader(is_leader);
  info->set_raft_role(raft_role);
  
  response->set_success(true);
  return grpc::Status::OK;
}

// =============================================================================
// Data Persistence
// =============================================================================

grpc::Status StorageServiceImpl::Flush(grpc::ServerContext* context,
                                        const cedar::storage::FlushRequest* request,
                                        cedar::storage::FlushResponse* response) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  auto status = partition_manager_->FlushAll();
  
  if (status.ok()) {
    response->set_success(true);
    // Estimate flushed size from partition stats (instrumentation needed for exact value)
    uint64_t total_data = 0;
    for (auto pid : partition_manager_->GetAllPartitions()) {
      auto* part = partition_manager_->GetPartition(pid);
      if (part) total_data += part->GetStats().disk_usage_bytes;
    }
    response->set_flushed_size(total_data);
    return grpc::Status::OK;
  } else {
    response->set_success(false);
    response->set_error_msg(status.ToString());
    return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
  }
}

// =============================================================================
// Heartbeat (Bidirectional Streaming)
// =============================================================================

grpc::Status StorageServiceImpl::Heartbeat(grpc::ServerContext* context,
                                            grpc::ServerReaderWriter<cedar::storage::HeartbeatResponse,
                                                                     cedar::storage::HeartbeatRequest>* stream) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }
  
  cedar::storage::HeartbeatRequest request;
  
  // Process incoming heartbeat requests and send responses
  while (stream->Read(&request)) {
    cedar::storage::HeartbeatResponse response;
    response.set_success(true);
    
    // Process any commands from the request
    // For now, just acknowledge
    
    if (!stream->Write(response)) {
      break;
    }
  }
  
  return grpc::Status::OK;
}


// =============================================================================
// QueryD Sub-Query Execution (Adaptive Execution Path)
// =============================================================================

grpc::Status StorageServiceImpl::ExecuteSubQuery(
    grpc::ServerContext* context,
    const cedar::storage::ExecuteSubQueryRequest* request,
    grpc::ServerWriter<cedar::storage::SubQueryResultBatch>* writer) {
  if (context->IsCancelled()) {
    return grpc::Status::CANCELLED;
  }

  // Leader check for linearizable read
  PartitionID pid = static_cast<PartitionID>(request->partition_id());
  std::string leader_hint;
  auto leader_status = CheckReadLeader(pid, &leader_hint);
  if (!leader_status.ok()) {
    cedar::storage::SubQueryResultBatch batch;
    batch.set_is_last(true);
    writer->Write(batch);
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "Not leader", leader_hint);
  }

  if (!cypher_engine_) {
    cedar::storage::SubQueryResultBatch batch;
    batch.set_is_last(true);
    writer->Write(batch);
    return grpc::Status::OK;
  }

  // Convert proto parameters to cypher::Value map
  std::map<std::string, cedar::cypher::Value> params;
  for (const auto& kv : request->parameters()) {
    const auto& qv = kv.second;
    switch (qv.value_type_case()) {
      case cedar::storage::QueryValue::kBoolVal:
        params[kv.first] = cedar::cypher::Value(qv.bool_val());
        break;
      case cedar::storage::QueryValue::kIntVal:
        params[kv.first] = cedar::cypher::Value(qv.int_val());
        break;
      case cedar::storage::QueryValue::kFloatVal:
        params[kv.first] = cedar::cypher::Value(qv.float_val());
        break;
      case cedar::storage::QueryValue::kStringVal:
        params[kv.first] = cedar::cypher::Value(qv.string_val());
        break;
      default:
        params[kv.first] = cedar::cypher::Value::Null();
        break;
    }
  }

  // Execute via CypherEngine
  cedar::cypher::ResultSet result = cypher_engine_->Execute(request->query_fragment(), params);

  cedar::storage::SubQueryResultBatch batch;
  if (result.HasError()) {
    batch.set_is_last(true);
    writer->Write(batch);
    return grpc::Status::OK;
  }

  // Write column names
  for (const auto& col : result.columns) {
    batch.add_columns(col);
  }

  // Write records
  for (const auto& record : result.records) {
    auto* row = batch.add_records();
    for (const auto& col : result.columns) {
      auto it = record.values.find(col);
      if (it != record.values.end()) {
        cedar::storage::QueryValue qv;
        switch (it->second.Type()) {
          case cedar::cypher::ValueType::kBool:
            qv.set_bool_val(it->second.GetBool());
            break;
          case cedar::cypher::ValueType::kInt:
            qv.set_int_val(it->second.GetInt());
            break;
          case cedar::cypher::ValueType::kFloat:
            qv.set_float_val(it->second.GetFloat());
            break;
          case cedar::cypher::ValueType::kString:
            qv.set_string_val(it->second.GetString());
            break;
          default:
            // For complex types (Node, Edge, List, Map), serialize as string
            qv.set_string_val("<complex>");
            break;
        }
        *row->add_values() = std::move(qv);
      } else {
        // Missing column -> null
        cedar::storage::QueryValue qv;
        *row->add_values() = std::move(qv);
      }
    }
  }

  batch.set_is_last(true);
  writer->Write(batch);
  return grpc::Status::OK;
}
}  // namespace dtx
}  // namespace cedar
