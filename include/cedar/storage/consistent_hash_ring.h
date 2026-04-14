#ifndef CEDAR_STORAGE_CONSISTENT_HASH_RING_H_
#define CEDAR_STORAGE_CONSISTENT_HASH_RING_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace cedar {
namespace storage {

struct VirtualNode {
  uint32_t hash;
  std::string physical_node;
  uint32_t replica_index;
  
  bool operator<(const VirtualNode& other) const {
    return hash < other.hash;
  }
};

struct HashRingConfig {
  uint32_t virtual_nodes_per_physical = 150;
  std::function<uint32_t(const std::string&)> hash_function;
  uint32_t replication_factor = 3;
};

class ConsistentHashRing {
 public:
  explicit ConsistentHashRing(const HashRingConfig& config);
  ~ConsistentHashRing() = default;

  void AddNode(const std::string& node_id);
  void RemoveNode(const std::string& node_id);
  
  std::string GetNode(const std::string& key);
  std::vector<std::string> GetNodes(const std::string& key, size_t n);
  
  std::vector<std::pair<std::string, std::string>> GetMigrationPlanForAdd(
      const std::string& new_node);
  
  std::vector<std::pair<std::string, std::string>> GetMigrationPlanForRemove(
      const std::string& removed_node);
  
  size_t Size() const;
  size_t PhysicalNodeCount() const;
  std::map<std::string, uint32_t> GetDistribution() const;

 private:
  uint32_t Hash(const std::string& key);
  void AddVirtualNodes(const std::string& node_id);
  void RemoveVirtualNodes(const std::string& node_id);

  HashRingConfig config_;
  std::map<uint32_t, VirtualNode> ring_;
  std::set<std::string> physical_nodes_;
  mutable std::mutex mutex_;
};

}  // namespace storage
}  // namespace cedar

#endif  // CEDAR_STORAGE_CONSISTENT_HASH_RING_H_
