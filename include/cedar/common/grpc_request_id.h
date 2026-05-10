// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");

#ifndef CEDAR_COMMON_GRPC_REQUEST_ID_H_
#define CEDAR_COMMON_GRPC_REQUEST_ID_H_

#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <string>
#include "cedar/common/json_logger.h"

namespace cedar {
namespace common {

static constexpr const char* kRequestIdMetadataKey = "x-request-id";

// Extract request ID from incoming gRPC metadata, or generate one
inline std::string ExtractOrGenerateRequestId(grpc::ServerContext* context) {
  auto metadata = context->client_metadata();
  auto it = metadata.find(kRequestIdMetadataKey);
  if (it != metadata.end()) {
    return std::string(it->second.data(), it->second.length());
  }
  return GenerateRequestId();
}

// Propagate request ID to outgoing gRPC client context
inline void PropagateRequestId(grpc::ClientContext* context) {
  std::string req_id = GetRequestId();
  if (!req_id.empty()) {
    context->AddMetadata(kRequestIdMetadataKey, req_id);
  }
}

}  // namespace common
}  // namespace cedar

#endif  // CEDAR_COMMON_GRPC_REQUEST_ID_H_
