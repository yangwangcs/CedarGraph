// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0

#ifndef CEDAR_SERVICE_GCN_ROUTE_CACHE_H_
#define CEDAR_SERVICE_GCN_ROUTE_CACHE_H_

#include <grpcpp/grpcpp.h>

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "cedar/core/status.h"
#include "cedar/dtx/meta_service.h"
#include "gcn_service.grpc.pb.h"

namespace cedar {
namespace service {

class GcnRouteCache {
 public:
  using LocateFn = std::function<StatusOr<cedar::dtx::GcnRoute>(uint32_t, uint64_t)>;
  using ChannelFactory = std::function<std::shared_ptr<grpc::Channel>(const std::string&)>;

  GcnRouteCache(LocateFn locate_fn, ChannelFactory channel_factory);

  StatusOr<cedar::gcn::TraversalResponse> Traverse(
      uint32_t partition_id,
      uint64_t required_version,
      const cedar::gcn::TraversalRequest& request);

  static bool IsAcceptableTraversalResponse(
      const cedar::dtx::GcnRoute& route,
      uint64_t required_version,
      const cedar::gcn::TraversalResponse& response,
      std::string* reason);

 private:
  std::shared_ptr<cedar::gcn::GcnService::Stub> GetStub(const std::string& endpoint);

  LocateFn locate_fn_;
  ChannelFactory channel_factory_;
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<cedar::gcn::GcnService::Stub>> stubs_;
};

}  // namespace service
}  // namespace cedar

#endif  // CEDAR_SERVICE_GCN_ROUTE_CACHE_H_
