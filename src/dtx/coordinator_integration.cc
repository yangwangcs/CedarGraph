#include "cedar/dtx/coordinator_integration.h"
#include "cedar/types/descriptor.h"

namespace cedar {
namespace dtx {

PartitionRouteCache::PartitionRouteCache() = default;

StatusOr<PartitionRoute> PartitionRouteCache::GetRoute(const std::string& space_name, 
                                                        PartitionID partition_id) {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    auto space_it = cache_.find(space_name);
    if (space_it != cache_.end()) {
        auto route_it = space_it->second.routes.find(partition_id);
        if (route_it != space_it->second.routes.end()) {
            if (!route_it->second.IsExpired()) {
                return route_it->second;
            }
        }
    }
    return Status::NotFound("Route not in cache");
}

void PartitionRouteCache::UpdateRoute(const std::string& space_name, 
                                      const PartitionRoute& route) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    cache_[space_name].routes[route.partition_id] = route;
}

void PartitionRouteCache::Invalidate(const std::string& space_name, PartitionID partition_id) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    auto space_it = cache_.find(space_name);
    if (space_it != cache_.end()) {
        space_it->second.routes.erase(partition_id);
    }
}

void PartitionRouteCache::InvalidateSpace(const std::string& space_name) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    cache_.erase(space_name);
}

void PartitionRouteCache::InvalidateAll() {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    cache_.clear();
}

Status PartitionRouteCache::PreloadSpace(const std::string& space_name, 
                                         MetaServiceClient* meta_client) {
    auto result = meta_client->GetSpacePartitionMap(space_name);
    if (!result.ok()) return result.status();
    const auto& partition_map = result.value();
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    auto& space_cache = cache_[space_name];
    space_cache.version = partition_map.version;
    for (const auto& [pid, assignment] : partition_map.assignments) {
        PartitionRoute route;
        route.partition_id = pid;
        route.leader_node = assignment.leader_node;
        route.version = assignment.version;
        route.cached_at = std::chrono::steady_clock::now();
        space_cache.routes[pid] = route;
    }
    return Status::OK();
}

StorageConnectionPool::StorageConnectionPool() = default;
StorageConnectionPool::~StorageConnectionPool() { CloseAll(); }

void StorageConnectionPool::MarkUnhealthy(NodeID node_id) {
    std::shared_lock<std::shared_mutex> lock(pool_mutex_);
    auto it = connections_.find(node_id);
    if (it != connections_.end()) {
        it->second->healthy_.store(false);
    }
}

void StorageConnectionPool::CloseAll() {
    running_ = false;
    if (health_check_thread_.joinable()) health_check_thread_.join();
    std::unique_lock<std::shared_mutex> lock(pool_mutex_);
    connections_.clear();
}

StorageConnectionPool::HealthStats StorageConnectionPool::GetHealthStats() const {
    std::shared_lock<std::shared_mutex> lock(pool_mutex_);
    HealthStats stats;
    stats.total_connections = connections_.size();
    for (const auto& [node_id, conn] : connections_) {
        if (conn->healthy_.load()) stats.healthy_connections++;
        else stats.unhealthy_connections++;
    }
    return stats;
}

void StorageConnectionPool::HealthCheckLoop() {}

IntegratedCoordinator::IntegratedCoordinator() = default;
IntegratedCoordinator::~IntegratedCoordinator() { Shutdown(); }

Status IntegratedCoordinator::Initialize(const IntegratedCoordinatorConfig& config) {
    config_ = config;
    auto status = InitializeMetaClient();
    CEDAR_RETURN_IF_ERROR(status);
    status = InitializeRouteCache();
    CEDAR_RETURN_IF_ERROR(status);
    DTxConfig dtx_config;
    lnd_engine_ = std::make_unique<LndOccEngine>(dtx_config);
    initialized_ = true;
    return Status::OK();
}

Status IntegratedCoordinator::Shutdown() {
    if (!initialized_) return Status::OK();
    {
        std::unique_lock<std::shared_mutex> lock(txns_mutex_);
        active_txns_.clear();
    }
    connection_pool_.CloseAll();
    initialized_ = false;
    return Status::OK();
}

Status IntegratedCoordinator::InitializeMetaClient() {
    meta_client_ = std::make_unique<MetaServiceClient>();
    auto status = meta_client_->Connect(config_.meta_addresses);
    CEDAR_RETURN_IF_ERROR(status);
    meta_client_->WatchPartitionMap(config_.space_name, 
        [this](const PartitionMapChange& change) { OnPartitionChange(change); });
    return Status::OK();
}

Status IntegratedCoordinator::InitializeRouteCache() {
    if (config_.preload_all_routes) {
        return route_cache_.PreloadSpace(config_.space_name, meta_client_.get());
    }
    return Status::OK();
}

void IntegratedCoordinator::OnPartitionChange(const PartitionMapChange& change) {
    if (change.partition_id == kInvalidPartitionID) {
        route_cache_.InvalidateSpace(change.space_name);
    } else {
        route_cache_.Invalidate(change.space_name, change.partition_id);
    }
}

StatusOr<TxnID> IntegratedCoordinator::BeginTransaction(const DistributedTxnOptions& options) {
    TxnID txn_id = next_txn_id_.fetch_add(1);
    auto ctx = std::make_unique<DistributedTxnContext>();
    ctx->SetTxnID(txn_id);
    ctx->SetStartTimestamp(0);
    ctx->SetCoordinator(0);
    {
        std::unique_lock<std::shared_mutex> lock(txns_mutex_);
        active_txns_[txn_id] = std::move(ctx);
    }
    return txn_id;
}

StatusOr<Descriptor> IntegratedCoordinator::Read(TxnID txn_id, const CedarKey& key) {
    auto ctx = GetTxnContext(txn_id);
    if (!ctx) return Status::NotFound("Transaction not found");
    auto partition_result = RouteKeyToPartition(key);
    if (!partition_result.ok()) return partition_result.status();
    auto route_result = GetKeyRoute(key);
    if (!route_result.ok()) return route_result.status();
    return Descriptor();  // placeholder
}

Status IntegratedCoordinator::Write(TxnID txn_id, const CedarKey& key, const Descriptor& value) {
    auto ctx = GetTxnContext(txn_id);
    if (!ctx) return Status::NotFound("Transaction not found");
    ctx->AddToWriteSet(key);
    return Status::OK();
}

StatusOr<CommitResult> IntegratedCoordinator::Commit(TxnID txn_id) {
    auto ctx = GetTxnContext(txn_id);
    if (!ctx) return Status::NotFound("Transaction not found");
    auto txn_type = lnd_engine_->ClassifyTransaction(ctx);
    ctx->SetType(txn_type);
    LndOccCommitResult result;
    std::vector<PartitionID> participants(ctx->GetParticipants().begin(), ctx->GetParticipants().end());
    switch (txn_type) {
        case TxnType::kSinglePartition:
            result = lnd_engine_->SinglePartitionCommit(ctx);
            break;
        case TxnType::kSameTemporalRange:
            result = lnd_engine_->SameTemporalRangeCommit(participants, ctx);
            break;
        case TxnType::kCrossTemporalRange:
            result = lnd_engine_->FullTwoPhaseCommit(participants, ctx);
            break;
        default:
            return Status::InvalidArgument("Unknown transaction type");
    }
    {
        std::unique_lock<std::shared_mutex> lock(txns_mutex_);
        active_txns_.erase(txn_id);
    }
    CommitResult commit_result;
    commit_result.success = result.success;
    return commit_result;
}

Status IntegratedCoordinator::Abort(TxnID txn_id) {
    auto ctx = GetTxnContext(txn_id);
    if (!ctx) return Status::NotFound("Transaction not found");
    {
        std::unique_lock<std::shared_mutex> lock(txns_mutex_);
        active_txns_.erase(txn_id);
    }
    return Status::OK();
}

StatusOr<PartitionRoute> IntegratedCoordinator::GetKeyRoute(const CedarKey& key) {
    auto partition_result = RouteKeyToPartition(key);
    if (!partition_result.ok()) return partition_result.status();
    auto partition_id = partition_result.value();
    auto cached_route = route_cache_.GetRoute(config_.space_name, partition_id);
    if (cached_route.ok()) {
        cache_hits_++;
        return cached_route.value();
    }
    cache_misses_++;
    auto assignment = meta_client_->GetPartitionAssignment(config_.space_name, partition_id);
    if (!assignment.ok()) return assignment.status();
    PartitionRoute route;
    route.partition_id = partition_id;
    route.leader_node = assignment.value().leader_node;
    route.version = assignment.value().version;
    route.cached_at = std::chrono::steady_clock::now();
    route_cache_.UpdateRoute(config_.space_name, route);
    return route;
}

std::vector<PartitionRoute> IntegratedCoordinator::GetKeysRoutes(const std::vector<CedarKey>& keys) {
    std::vector<PartitionRoute> routes;
    routes.reserve(keys.size());
    for (const auto& key : keys) {
        auto route = GetKeyRoute(key);
        if (route.ok()) routes.push_back(route.value());
    }
    return routes;
}

void IntegratedCoordinator::RefreshRouteCache() {
    route_cache_.InvalidateSpace(config_.space_name);
    InitializeRouteCache();
}

IntegratedCoordinator::CacheStats IntegratedCoordinator::GetCacheStats() const {
    CacheStats stats;
    stats.cache_hits = cache_hits_.load();
    stats.cache_misses = cache_misses_.load();
    return stats;
}

DistributedTxnContext* IntegratedCoordinator::GetTxnContext(TxnID txn_id) {
    std::shared_lock<std::shared_mutex> lock(txns_mutex_);
    auto it = active_txns_.find(txn_id);
    if (it != active_txns_.end()) return it->second.get();
    return nullptr;
}

StatusOr<PartitionID> IntegratedCoordinator::RouteKeyToPartition(const CedarKey& key) {
    auto result = meta_client_->GetSpacePartitionMap(config_.space_name);
    if (!result.ok()) return result.status();
    return result.value().GetPartitionForKey(key);
}

} // namespace dtx
} // namespace cedar
