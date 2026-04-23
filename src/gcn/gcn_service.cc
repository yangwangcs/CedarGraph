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
// GcnService Implementation - Graph Compute Node RPC Service Stub
// =============================================================================

#include "cedar/gcn/gcn_service.h"

namespace cedar {
namespace gcn {

GcnServiceImpl::GcnServiceImpl() = default;
GcnServiceImpl::~GcnServiceImpl() = default;

grpc::Status GcnServiceImpl::Traverse(grpc::ServerContext* /*context*/,
                                      const TraversalRequest* /*request*/,
                                      TraversalResponse* response) {
  // Stub: return default response fields
  response->Clear();
  return grpc::Status::OK;
}

grpc::Status GcnServiceImpl::SubQuery(grpc::ServerContext* /*context*/,
                                      const SubQueryRequest* /*request*/,
                                      SubQueryResponse* response) {
  // Stub: return default response fields
  response->Clear();
  return grpc::Status::OK;
}

grpc::Status GcnServiceImpl::OnCacheInvalidate(grpc::ServerContext* /*context*/,
                                               const CacheInvalidateNotice* /*request*/,
                                               Empty* response) {
  // Stub: return empty response
  response->Clear();
  return grpc::Status::OK;
}

grpc::Status GcnServiceImpl::OnEventStream(
    grpc::ServerContext* /*context*/,
    grpc::ServerReaderWriter<CDCEvent, Ack>* /*stream*/) {
  // Stub: close stream immediately
  return grpc::Status::OK;
}

}  // namespace gcn
}  // namespace cedar
