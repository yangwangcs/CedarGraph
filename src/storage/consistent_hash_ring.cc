#include "cedar/storage/consistent_hash_ring.h"

#include <openssl/evp.h>
#include <set>

namespace cedar {
namespace storage {

uint32_t DefaultHash(const std::string& key) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len;
  
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  EVP_DigestInit_ex(ctx, EVP_md5(), nullptr);
  EVP_DigestUpdate(ctx, key.c_str(), key.length());
  EVP_DigestFinal_ex(ctx, digest, &digest_len);
  EVP_MD_CTX_free(ctx);
  
  return (static_cast<uint32_t>(digest[0]) << 24) |
         (static_cast<uint32_t>(digest[1]) << 16) |
         (static_cast<uint32_t>(digest[2]) << 8) |
         static_cast<uint32_t>(digest[3]);
}

ConsistentHashRing::ConsistentHashRing(const HashRingConfig& config) 
    : config_(config) {
  if (!config_.hash_function) {
    config_.hash_function = DefaultHash;
  }
}

void ConsistentHashRing::AddNode(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (physical_nodes_.find(node_id) != physical_nodes_.end()) {
    return;
  }
  
  physical_nodes_.insert(node_id);
  AddVirtualNodes(node_id);
}

void ConsistentHashRing::RemoveNode(const std::string& node_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (physical_nodes_.find(node_id) == physical_nodes_.end()) {
    return;
  }
  
  physical_nodes_.erase(node_id);
  RemoveVirtualNodes(node_id);
}

std::string ConsistentHashRing::GetNode(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (ring_.empty()) {
    return "";
  }
  
  uint32_t hash = Hash(key);
  
  auto it = ring_.lower_bound(hash);
  if (it == ring_.end()) {
    it = ring_.begin();
  }
  
  return it->second.physical_node;
}

std::vector<std::string> ConsistentHashRing::GetNodes(const std::string& key, 
                                                       size_t n) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<std::string> result;
  if (ring_.empty() || n == 0) {
    return result;
  }
  
  uint32_t hash = Hash(key);
  auto it = ring_.lower_bound(hash);
  
  std::set<std::string> seen;
  while (result.size() < n && seen.size() < physical_nodes_.size()) {
    if (it == ring_.end()) {
      it = ring_.begin();
    }
    
    const std::string& node = it->second.physical_node;
    if (seen.find(node) == seen.end()) {
      result.push_back(node);
      seen.insert(node);
    }
    
    ++it;
  }
  
  return result;
}

std::vector<std::pair<std::string, std::string>> 
ConsistentHashRing::GetMigrationPlanForAdd(const std::string& new_node) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<std::pair<std::string, std::string>> migrations;
  
  // Simulate: find which keys would move to the new node
  // This is a simplified implementation
  
  return migrations;
}

std::vector<std::pair<std::string, std::string>> 
ConsistentHashRing::GetMigrationPlanForRemove(const std::string& removed_node) {
  std::vector<std::pair<std::string, std::string>> migrations;
  
  return migrations;
}

size_t ConsistentHashRing::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return ring_.size();
}

size_t ConsistentHashRing::PhysicalNodeCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return physical_nodes_.size();
}

std::map<std::string, uint32_t> ConsistentHashRing::GetDistribution() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::map<std::string, uint32_t> dist;
  for (const auto& [hash, vnode] : ring_) {
    dist[vnode.physical_node]++;
  }
  
  return dist;
}

uint32_t ConsistentHashRing::Hash(const std::string& key) {
  return config_.hash_function(key);
}

void ConsistentHashRing::AddVirtualNodes(const std::string& node_id) {
  for (uint32_t i = 0; i < config_.virtual_nodes_per_physical; i++) {
    std::string vnode_key = node_id + "#" + std::to_string(i);
    uint32_t hash = Hash(vnode_key);
    
    VirtualNode vnode;
    vnode.hash = hash;
    vnode.physical_node = node_id;
    vnode.replica_index = i;
    
    ring_[hash] = vnode;
  }
}

void ConsistentHashRing::RemoveVirtualNodes(const std::string& node_id) {
  for (auto it = ring_.begin(); it != ring_.end(); ) {
    if (it->second.physical_node == node_id) {
      it = ring_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace storage
}  // namespace cedar
