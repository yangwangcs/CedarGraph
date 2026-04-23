// tools/graphcomputenode.cc
#include <iostream>
#include <gflags/gflags.h>

DEFINE_int32(port, 9780, "GCN service port");
DEFINE_string(coordinator, "127.0.0.1:9559", "Coordinator endpoint");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << "graphcomputenode starting on port " << FLAGS_port << std::endl;
  // TODO: full initialization in Task 3.1
  return 0;
}
