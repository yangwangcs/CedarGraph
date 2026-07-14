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

#include <csignal>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>

#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/gcn/gcn_node.h"
#include "cedar/gcn/storage_cdc_client.h"

DECLARE_int32(gcn_port);
DECLARE_string(gcn_bind_address);
DECLARE_string(gcn_coordinator);
DECLARE_string(gcn_checkpoint_dir);

DEFINE_string(gcn_partition_ids, "",
              "Comma-separated StorageD partition IDs this GCN should consume");
DEFINE_string(gcn_partition_epochs, "",
              "Comma-separated partition epochs matching --gcn_partition_ids; "
              "defaults each epoch to 0 when omitted");
DEFINE_string(gcn_storage_endpoints, "",
              "Comma-separated StorageD gRPC endpoints matching "
              "--gcn_partition_ids");
DEFINE_int32(gcn_cdc_poll_interval_ms, 50,
             "CDC poll interval for standalone GCN partition consumers");
DEFINE_bool(gcn_use_metad_leases, false,
            "Use MetaD dynamic GCN leases instead of static partition leases");
DEFINE_uint64(gcn_node_id, 0,
              "Stable GCN node id for MetaD lease registration; defaults to port");
DEFINE_uint64(gcn_incarnation, 0,
              "GCN process incarnation for MetaD lease registration; defaults to start time");
DEFINE_string(gcn_advertised_endpoint, "",
              "GCN endpoint advertised to MetaD; defaults to bind_address:port");
DEFINE_int32(gcn_lease_renew_interval_ms, 5000,
             "MetaD GCN lease renewal interval in milliseconds");

std::atomic<bool> g_shutdown{false};

void SignalHandler(int sig) {
  std::cout << "\n[GCN] Received signal " << sig << ", shutting down..." << std::endl;
  g_shutdown = true;
}

namespace {

std::vector<std::string> SplitCommaSeparated(const std::string& value) {
  std::vector<std::string> parts;
  std::stringstream stream(value);
  std::string part;
  while (std::getline(stream, part, ',')) {
    if (!part.empty()) {
      parts.push_back(part);
    }
  }
  return parts;
}

bool ParseUint64(const std::string& value, uint64_t* out) {
  try {
    size_t parsed = 0;
    uint64_t result = std::stoull(value, &parsed, 10);
    if (parsed != value.size()) {
      return false;
    }
    *out = result;
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool BuildCdcOptions(cedar::GcnNode::Options* options) {
  if (FLAGS_gcn_cdc_poll_interval_ms <= 0) {
    std::cerr << "[GCN] --gcn_cdc_poll_interval_ms must be positive"
              << std::endl;
    return false;
  }

  options->bind_address = FLAGS_gcn_bind_address;
  options->port = FLAGS_gcn_port;
  options->coordinator_endpoint = FLAGS_gcn_coordinator;
  options->checkpoint_directory = FLAGS_gcn_checkpoint_dir;
  options->cdc_poll_interval =
      std::chrono::milliseconds(FLAGS_gcn_cdc_poll_interval_ms);
  if (FLAGS_gcn_lease_renew_interval_ms <= 0) {
    std::cerr << "[GCN] --gcn_lease_renew_interval_ms must be positive"
              << std::endl;
    return false;
  }
  options->lease_renew_interval =
      std::chrono::milliseconds(FLAGS_gcn_lease_renew_interval_ms);
  options->gcn_id = FLAGS_gcn_node_id;
  options->gcn_incarnation = FLAGS_gcn_incarnation;
  options->advertised_endpoint = FLAGS_gcn_advertised_endpoint;
  options->use_metad_leases = FLAGS_gcn_use_metad_leases;

  const auto partition_ids = SplitCommaSeparated(FLAGS_gcn_partition_ids);
  const auto partition_epochs = SplitCommaSeparated(FLAGS_gcn_partition_epochs);
  const auto storage_endpoints = SplitCommaSeparated(FLAGS_gcn_storage_endpoints);

  if (partition_ids.empty() && storage_endpoints.empty()) {
    return true;
  }
  if (partition_ids.empty() || partition_ids.size() != storage_endpoints.size()) {
    std::cerr << "[GCN] --gcn_partition_ids and --gcn_storage_endpoints must "
                 "both be set with the same number of entries"
              << std::endl;
    return false;
  }
  if (!partition_epochs.empty() &&
      partition_epochs.size() != partition_ids.size()) {
    std::cerr << "[GCN] --gcn_partition_epochs must be omitted or match "
                 "--gcn_partition_ids length"
              << std::endl;
    return false;
  }

  auto creds =
      cedar::dtx::raft::TlsCredentialFactory::CreateClientCredentialsFromEnvStrict();
  if (!creds.ok()) {
    std::cerr << "[GCN] Failed to create StorageD TLS credentials: "
              << creds.status().ToString() << std::endl;
    return false;
  }

  std::unordered_set<uint32_t> seen_partitions;
  for (size_t i = 0; i < partition_ids.size(); ++i) {
    uint64_t partition_id = 0;
    if (!ParseUint64(partition_ids[i], &partition_id) ||
        partition_id > std::numeric_limits<uint32_t>::max()) {
      std::cerr << "[GCN] Invalid partition id: " << partition_ids[i]
                << std::endl;
      return false;
    }

    uint64_t partition_epoch = 0;
    if (!partition_epochs.empty() &&
        !ParseUint64(partition_epochs[i], &partition_epoch)) {
      std::cerr << "[GCN] Invalid partition epoch: " << partition_epochs[i]
                << std::endl;
      return false;
    }

    cedar::gcn::PartitionLease lease;
    lease.partition_id = static_cast<uint32_t>(partition_id);
    if (!seen_partitions.insert(lease.partition_id).second) {
      std::cerr << "[GCN] Duplicate partition id: " << lease.partition_id
                << std::endl;
      return false;
    }
    lease.partition_epoch = partition_epoch;
    lease.lease_epoch = partition_epoch;
    if (!FLAGS_gcn_use_metad_leases) {
      options->partition_leases.push_back(lease);
    }

    auto channel = grpc::CreateChannel(storage_endpoints[i],
                                       creds.ValueOrDie());
    cedar::gcn::StorageCdcClient::Options client_options;
    options->storage_cdc_sources.emplace(
        lease.partition_id,
        std::make_shared<cedar::gcn::StorageCdcClient>(channel,
                                                       client_options));
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  cedar::GcnNode::Options options;
  if (!BuildCdcOptions(&options)) {
    return 1;
  }

  cedar::GcnNode node(std::move(options));
  auto init_status = node.Initialize();
  if (!init_status.ok()) {
    std::cerr << "[GCN] Failed to initialize: "
              << init_status.ToString() << std::endl;
    return 1;
  }

  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  auto start_status = node.Start();
  if (!start_status.ok()) {
    std::cerr << "[GCN] Failed to start: " << start_status.ToString()
              << std::endl;
    node.Stop().IgnoreError();
    return 1;
  }

  std::cout << "[GCN] Running. Press Ctrl+C to stop." << std::endl;

  // Block until a shutdown signal is received
  while (!g_shutdown.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  node.Stop();
  std::cout << "[GCN] Stopped." << std::endl;
  return 0;
}
