// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Minimal cedar-queryd - 简化版用于编译测试

#include <iostream>
#include <memory>
#include <string>

#include "cedar/core/status.h"
#include "cedar/dtx/storage_service_impl.h"

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;
  
  std::cout << "CedarGraph Query Daemon (minimal version)" << std::endl;
  std::cout << "This is a stub implementation for compilation testing." << std::endl;
  
  // 测试基础组件是否能链接
  cedar::dtx::StorageClient client;
  std::cout << "StorageClient created: " << (client.IsConnected() ? "connected" : "not connected") << std::endl;
  
  std::cout << " cedar-queryd minimal version started successfully!" << std::endl;
  return 0;
}
