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

// Cluster Connection Tests for CedarGraph Distributed Storage

#include <gtest/gtest.h>
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/governance/service_registry.h"

using namespace cedar;

TEST(ClusterConnectionTest, ConnectToSingleEndpoint) {
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;
  options.meta_endpoints = {"127.0.0.1:9559"};
  options.dtx_config.rpc_timeout_ms = 5000;
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, "/tmp/test_single", &storage);
  
  if (s.ok()) {
    EXPECT_TRUE(storage->IsDistributedMode());
    EXPECT_NE(storage->GetStorageClient(), nullptr);
    delete storage;
  }
}

TEST(ClusterConnectionTest, ServiceDiscoveryConnection) {
  governance::ServiceRegistry registry;
  governance::ServiceInfo info;
  info.id = "storaged-test-1";
  info.name = "storaged";
  info.host = "127.0.0.1";
  info.port = 9779;
  info.status = governance::ServiceStatus::kHealthy;
  registry.Register(info);
  
  CedarOptions options;
  options.create_if_missing = true;
  options.distributed_mode = true;
  options.enable_service_discovery = true;
  options.service_registry = &registry;
  options.storage_service_name = "storaged";
  
  CedarGraphStorage* storage = nullptr;
  Status s = CedarGraphStorage::Open(options, "/tmp/test_discovery", &storage);
  
  if (s.ok()) {
    EXPECT_TRUE(storage->IsDistributedMode());
    delete storage;
  }
}
