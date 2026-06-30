// Client Library Test
// Tests the unified client architecture (without actual network connections)

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <cmath>
#include <sstream>
#include <thread>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include "cedar/client/auto_scaler.h"
#include "cedar/client/cluster_backup.h"
#include "cedar/client/cluster_manager.h"
#include "cedar/client/cluster_monitor.h"
#include "cedar/client/cedar_client.h"
#include "cedar/client/config_loader.h"
#include "cedar/client/config_watcher.h"
#include "cedar/client/docker_manager.h"
#include "cedar/client/jwt_manager.h"
#include "cedar/client/k8s_manager.h"
#include "cedar/client/service_discovery.h"

using namespace cedar::client;

namespace {

std::string TestBase64Encode(const std::string& input) {
  static const char base64_chars[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  std::string result;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];

  for (char c : input) {
    char_array_3[i++] = c;
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for (i = 0; i < 4; i++) {
        result += base64_chars[char_array_4[i]];
      }
      i = 0;
    }
  }

  if (i) {
    for (j = i; j < 3; j++) {
      char_array_3[j] = '\0';
    }

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);

    for (j = 0; j < i + 1; j++) {
      result += base64_chars[char_array_4[j]];
    }

    while (i++ < 3) {
      result += '=';
    }
  }

  return result;
}

std::string BuildSignedJwtForTest(const JWTConfig& config,
                                  const std::string& header,
                                  const std::string& issuer,
                                  const std::string& audience) {
  auto now = std::chrono::system_clock::now();
  auto expiration = now + std::chrono::seconds(3600);
  std::stringstream payload_ss;
  payload_ss << "{"
             << "\"sub\":\"u42\","
             << "\"name\":\"Grace\","
             << "\"iss\":\"" << issuer << "\","
             << "\"aud\":\"" << audience << "\","
             << "\"iat\":" << std::chrono::system_clock::to_time_t(now) << ","
             << "\"exp\":" << std::chrono::system_clock::to_time_t(expiration)
             << "}";

  std::string encoded_header = TestBase64Encode(header);
  std::string encoded_payload = TestBase64Encode(payload_ss.str());
  std::string data = encoded_header + "." + encoded_payload;

  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len = 0;
  HMAC(EVP_sha256(),
       config.secret_key.data(),
       static_cast<int>(config.secret_key.size()),
       reinterpret_cast<const unsigned char*>(data.data()),
       data.size(),
       digest,
       &digest_len);

  return data + "." +
         TestBase64Encode(std::string(reinterpret_cast<char*>(digest), digest_len));
}

}  // namespace

class ClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create client with default config (no actual connections)
    CedarClientConfig config;
    config.metad_host = "localhost";
    config.metad_port = 10559;
    config.graphd_host = "localhost";
    config.graphd_port = 9669;
    config.storaged_host = "localhost";
    config.storaged_port = 9779;
    config.enable_service_discovery = false;  // Disable for testing
    config.max_connections = 2;  // Small pool for testing
    config.timeout_ms = 100;  // Short timeout for testing
    config.enable_logging = false;  // Disable logging for testing
    config.enable_config_watcher = false;  // Disable config watcher for testing
    
    client_ = std::make_unique<CedarClient>(config);
    client_->Initialize();
  }
  
  void TearDown() override {
    // Nothing to cleanup
  }

  std::unique_ptr<CedarClient> client_;
};

TEST_F(ClientTest, Initialization) {
  // Test client initialization (already initialized in SetUp)
  EXPECT_TRUE(client_->IsHealthy());
}

TEST_F(ClientTest, DefaultConfigUsesMetaDGrpcPort) {
  CedarClientConfig config;
  EXPECT_EQ(config.metad_port, 10559);
}

TEST_F(ClientTest, SessionManagement) {
  // Test session creation
  std::string session_id = client_->CreateSession("test_user");
  EXPECT_FALSE(session_id.empty());
  
  // Test session space
  client_->SetSessionSpace(session_id, "test_space");
  EXPECT_EQ(client_->GetSessionSpace(session_id), "test_space");
  
  // Test transaction
  std::string txn_id = client_->StartTransaction(session_id);
  EXPECT_FALSE(txn_id.empty());
  
  client_->EndTransaction(session_id);
}

TEST_F(ClientTest, DDLOperations) {
  auto result = client_->CreateSpace("test_space");
  EXPECT_FALSE(result.success);
  
  result = client_->CreateTag("test_space", "Person", {{"name", "string"}, {"age", "int"}});
  EXPECT_FALSE(result.success);
  EXPECT_NE(result.error_message.find("not yet implemented"), std::string::npos);
  
  result = client_->CreateEdge("test_space", "KNOWS", {{"since", "int"}});
  EXPECT_FALSE(result.success);
  EXPECT_NE(result.error_message.find("not yet implemented"), std::string::npos);

  EXPECT_FALSE(client_->DropSpace("test_space"));
}

TEST_F(ClientTest, InformationQueries) {
  auto spaces = client_->ListSpaces();
  EXPECT_TRUE(spaces.empty());

  auto edges = client_->ListEdges("test_space");
  EXPECT_TRUE(edges.empty());
}

TEST_F(ClientTest, Statistics) {
  // Initialize client first
  
  // Test statistics collection
  auto stats = client_->GetStats();
  EXPECT_EQ(stats.total_queries, 0);
}

TEST_F(ClientTest, Metrics) {
  // Initialize client first
  
  // Test metrics collection
  auto query_metrics = client_->GetQueryMetrics();
  EXPECT_EQ(query_metrics.total_queries, 0);
  
  auto conn_metrics = client_->GetConnectionMetrics();
  EXPECT_EQ(conn_metrics.total_connections, 0);
}

TEST_F(ClientTest, LoadBalancer) {
  // Test load balancer strategies
  LoadBalancer lb_round_robin(LoadBalancingStrategy::ROUND_ROBIN);
  LoadBalancer lb_weighted(LoadBalancingStrategy::WEIGHTED);
  LoadBalancer lb_least_conn(LoadBalancingStrategy::LEAST_CONNECTIONS);
  LoadBalancer lb_random(LoadBalancingStrategy::RANDOM);
  
  // Update nodes
  std::vector<LoadBalancerNode> nodes = {
    {"localhost", 9669, 1, 0, true},
    {"localhost", 9670, 2, 0, true},
    {"localhost", 9671, 3, 0, true}
  };
  
  lb_round_robin.UpdateNodes(nodes);
  lb_weighted.UpdateNodes(nodes);
  lb_least_conn.UpdateNodes(nodes);
  lb_random.UpdateNodes(nodes);
  
  // Select nodes
  auto node1 = lb_round_robin.SelectNode();
  auto node2 = lb_round_robin.SelectNode();
  auto node3 = lb_round_robin.SelectNode();
  
  // Round-robin should cycle through nodes
  EXPECT_NE(node1.host, "");
  EXPECT_NE(node2.host, "");
  EXPECT_NE(node3.host, "");
}

TEST_F(ClientTest, LoadBalancerDoesNotRouteToUnhealthyNodes) {
  std::vector<LoadBalancerNode> nodes = {
    {"localhost", 9669, 1, 0, false},
    {"localhost", 9670, 2, 0, false}
  };

  for (auto strategy : {LoadBalancingStrategy::ROUND_ROBIN,
                        LoadBalancingStrategy::WEIGHTED,
                        LoadBalancingStrategy::LEAST_CONNECTIONS,
                        LoadBalancingStrategy::RANDOM}) {
    LoadBalancer lb(strategy);
    lb.UpdateNodes(nodes);
    auto selected = lb.SelectNode();
    EXPECT_TRUE(selected.host.empty());
    EXPECT_EQ(selected.port, 0);
  }
}

TEST(QueryRouterTest, FailsWhenLoadBalancerHasNoHealthyNodes) {
  auto discovery = std::make_shared<ServiceDiscovery>(ServiceDiscoveryConfig{});
  auto load_balancer = std::make_shared<LoadBalancer>();
  load_balancer->UpdateNodes({{"127.0.0.1", 9669, 1, 0, false}});
  QueryRouter router(discovery, load_balancer);

  auto result = router.RouteByType(QueryType::ADMIN, "default");
  EXPECT_FALSE(result.success);
  EXPECT_EQ(result.error_message, "No available nodes");
}

TEST_F(ClientTest, ServiceDiscoveryRejectsInvalidConfig) {
  ServiceDiscoveryConfig config;
  config.metad_host = "";
  config.metad_port = 10559;

  ServiceDiscovery discovery(config);
  EXPECT_FALSE(discovery.Initialize());
}

TEST_F(ClientTest, ServiceDiscoveryRejectsNonPositiveIntervals) {
  ServiceDiscoveryConfig config;
  config.metad_host = "localhost";
  config.metad_port = 10559;
  config.refresh_interval_ms = 0;

  ServiceDiscovery discovery(config);
  EXPECT_FALSE(discovery.Initialize());
}

TEST_F(ClientTest, RetryHandler) {
  // Test retry handler
  RetryConfig config;
  config.max_retries = 3;
  config.initial_backoff_ms = 10;
  config.max_backoff_ms = 100;
  
  RetryHandler handler(config);
  
  // Test successful execution
  int attempts = 0;
  auto result = handler.Execute([&]() -> QueryResult {
    attempts++;
    QueryResult r;
    r.success = true;
    return r;
  });
  
  EXPECT_TRUE(result.success);
  EXPECT_EQ(result.attempts, 1);
  
  // Test failed execution with retry
  attempts = 0;
  result = handler.Execute([&]() -> QueryResult {
    attempts++;
    QueryResult r;
    r.success = false;
    r.error_message = "Test error";
    return r;
  });
  
  EXPECT_FALSE(result.success);
  EXPECT_EQ(result.attempts, 4);  // 1 initial + 3 retries
}

TEST_F(ClientTest, RetryHandlerClampsInvalidRetryConfig) {
  RetryConfig config;
  config.max_retries = -3;
  config.initial_backoff_ms = -10;
  config.max_backoff_ms = -1;
  config.backoff_multiplier = 0.0;

  RetryHandler handler(config);

  int attempts = 0;
  auto result = handler.Execute([&]() -> QueryResult {
    attempts++;
    QueryResult r;
    r.success = false;
    r.error_message = "Test error";
    return r;
  });

  EXPECT_FALSE(result.success);
  EXPECT_EQ(result.attempts, 1);
  EXPECT_EQ(attempts, 1);
}

TEST_F(ClientTest, RetryHandlerSaturatesBackoffWithoutOverflow) {
  RetryConfig config;
  config.max_retries = 1;
  config.initial_backoff_ms = 1;
  config.max_backoff_ms = 5;
  config.backoff_multiplier = std::numeric_limits<double>::infinity();

  RetryHandler handler(config);

  int attempts = 0;
  auto result = handler.Execute(
      [&]() -> QueryResult {
        attempts++;
        QueryResult r;
        r.success = attempts == 2;
        r.error_message = "Test error";
        return r;
      },
      [](const std::string&) { return true; });

  EXPECT_TRUE(result.success);
  EXPECT_EQ(result.attempts, 2);
  EXPECT_LT(result.total_time_ms, 1000);
}

TEST_F(ClientTest, ConnectionPool) {
  // Test connection pool
  ConnectionConfig config;
  config.host = "localhost";
  config.port = 9559;
  config.max_connections = 5;
  config.timeout_ms = 1000;
  
  ConnectionPool pool(config);
  
  // Check pool stats - connections are created lazily
  EXPECT_EQ(pool.GetActiveConnections(), 0);
  EXPECT_EQ(pool.GetAvailableConnections(), 0);
  EXPECT_EQ(pool.GetTotalConnections(), 0);
  EXPECT_TRUE(pool.IsHealthy());
}

TEST_F(ClientTest, SessionManager) {
  // Test session manager directly
  SessionManager manager;
  
  // Create session
  std::string session_id = manager.CreateSession("test_user");
  EXPECT_FALSE(session_id.empty());
  
  // Get session
  auto session = manager.GetSession(session_id);
  EXPECT_EQ(session.user_name, "test_user");
  EXPECT_EQ(session.current_space, "default");
  
  // Set space
  manager.SetSessionSpace(session_id, "test_space");
  EXPECT_EQ(manager.GetSessionSpace(session_id), "test_space");
  
  // Transaction
  std::string txn_id = manager.StartTransaction(session_id);
  EXPECT_FALSE(txn_id.empty());
  EXPECT_TRUE(manager.IsInTransaction(session_id));
  
  manager.EndTransaction(session_id);
  EXPECT_FALSE(manager.IsInTransaction(session_id));
  
  // Remove session
  manager.RemoveSession(session_id);
  EXPECT_EQ(manager.GetSessionCount(), 0);
}

TEST_F(ClientTest, MetricsCollector) {
  // Test metrics collector directly
  MetricsCollector collector;
  
  // Record query
  collector.RecordQuery("MATCH", 10, true);
  collector.RecordQuery("MATCH", 20, true);
  collector.RecordQuery("CREATE", 30, false);
  
  auto query_metrics = collector.GetQueryMetrics();
  EXPECT_EQ(query_metrics.total_queries, 3);
  EXPECT_EQ(query_metrics.successful_queries, 2);
  EXPECT_EQ(query_metrics.failed_queries, 1);
  EXPECT_EQ(query_metrics.min_latency_ms, 10);
  EXPECT_EQ(query_metrics.max_latency_ms, 30);
  
  // Record connection
  collector.RecordConnection("create");
  collector.RecordConnection("create");
  collector.RecordConnection("close");
  
  auto conn_metrics = collector.GetConnectionMetrics();
  EXPECT_EQ(conn_metrics.total_connections, 2);
  EXPECT_EQ(conn_metrics.active_connections, 1);
  
  // Reset
  collector.Reset();
  query_metrics = collector.GetQueryMetrics();
  EXPECT_EQ(query_metrics.total_queries, 0);
}

TEST(QueryRouterTest, ClassifiesCypherCreateAsWriteNotDdl) {
  auto discovery = std::make_shared<ServiceDiscovery>(ServiceDiscoveryConfig{});
  auto load_balancer = std::make_shared<LoadBalancer>(LoadBalancingStrategy::ROUND_ROBIN);
  QueryRouter router(discovery, load_balancer);

  EXPECT_EQ(router.GetQueryType("CREATE (n:Person {name: 'Ada'})"),
            QueryType::WRITE);
  EXPECT_EQ(router.GetQueryType("  merge (n:Person {id: 1})"),
            QueryType::WRITE);
  EXPECT_EQ(router.GetQueryType("CREATE SPACE demo"),
            QueryType::DDL);
  EXPECT_EQ(router.GetQueryType("DROP INDEX person_name"),
            QueryType::DDL);
}

TEST(ClusterBackupTest, UnknownComponentFailsWithoutLeavingInProgressStatus) {
  auto backup_dir = std::filesystem::temp_directory_path() / "cedar_cluster_backup_unknown";
  std::filesystem::remove_all(backup_dir);

  BackupConfig config;
  config.backup_dir = backup_dir.string();
  config.compress = false;

  ClusterBackup backup;
  ASSERT_TRUE(backup.Initialize(config, nullptr));

  auto info = backup.CreateBackup("graphd", BackupType::FULL);
  EXPECT_EQ(info.status, BackupStatus::FAILED);
  EXPECT_FALSE(info.error_message.empty());

  auto backups = backup.ListBackups();
  ASSERT_EQ(backups.size(), 1);
  EXPECT_EQ(backups[0].status, BackupStatus::FAILED);

  std::filesystem::remove_all(backup_dir);
}

TEST(ClusterBackupTest, OperationsFailClosedBeforeInitialize) {
  ClusterBackup backup;

  auto info = backup.CreateBackup("graphd", BackupType::FULL);
  EXPECT_EQ(info.status, BackupStatus::FAILED);
  EXPECT_NE(info.error_message.find("not initialized"), std::string::npos);

  RestoreOptions options;
  options.backup_id = "missing";
  EXPECT_FALSE(backup.RestoreBackup(options));
  EXPECT_FALSE(backup.UploadToS3("missing"));
  EXPECT_FALSE(backup.DownloadFromS3("missing"));
  EXPECT_FALSE(backup.CleanupOldBackups());
}


TEST(ClusterBackupTest, KnownComponentsFailClosedUntilClientBackupIsImplemented) {
  auto backup_dir = std::filesystem::temp_directory_path() / "cedar_cluster_backup_known";
  std::filesystem::remove_all(backup_dir);

  BackupConfig config;
  config.backup_dir = backup_dir.string();
  config.compress = false;

  ClusterBackup backup;
  ASSERT_TRUE(backup.Initialize(config, nullptr));

  for (const auto* component : {"metad", "storaged", "graphd", "all"}) {
    auto info = backup.CreateBackup(component, BackupType::FULL);
    EXPECT_EQ(info.status, BackupStatus::FAILED);
    EXPECT_NE(info.error_message.find("not implemented"), std::string::npos);
  }

  auto backups = backup.ListBackups();
  ASSERT_EQ(backups.size(), 4);
  for (const auto& info : backups) {
    EXPECT_EQ(info.status, BackupStatus::FAILED);
  }

  std::filesystem::remove_all(backup_dir);
}

TEST(ClusterBackupTest, RejectsUnsafeS3ArgumentsBeforeShellingOut) {
  auto backup_dir = std::filesystem::temp_directory_path() / "cedar_cluster_backup_s3";
  std::filesystem::remove_all(backup_dir);

  BackupConfig config;
  config.backup_dir = backup_dir.string();
  config.compress = false;
  config.s3_bucket = "bad;bucket";
  config.s3_prefix = "snapshots";

  ClusterBackup backup;
  ASSERT_TRUE(backup.Initialize(config, nullptr));

  EXPECT_FALSE(backup.DownloadFromS3("backup_1"));
  EXPECT_FALSE(backup.UploadToS3("backup;1"));

  std::filesystem::remove_all(backup_dir);
}

TEST(ClusterBackupTest, S3OperationsFailClosedWithoutCompletedVerifiedBackup) {
  auto backup_dir = std::filesystem::temp_directory_path() / "cedar_cluster_backup_s3_closed";
  std::filesystem::remove_all(backup_dir);

  BackupConfig config;
  config.backup_dir = backup_dir.string();
  config.compress = false;
  config.s3_bucket = "cedar-backups";
  config.s3_prefix = "snapshots";

  ClusterBackup backup;
  ASSERT_TRUE(backup.Initialize(config, nullptr));

  auto info = backup.CreateBackup("graphd", BackupType::FULL);
  EXPECT_EQ(info.status, BackupStatus::FAILED);

  EXPECT_FALSE(backup.UploadToS3(info.backup_id));
  EXPECT_FALSE(backup.DownloadFromS3("backup_1"));

  std::filesystem::remove_all(backup_dir);
}


TEST(ClusterBackupTest, EncryptionOptionFailsClosedUntilImplemented) {
  auto backup_dir = std::filesystem::temp_directory_path() / "cedar_cluster_backup_encrypt";
  std::filesystem::remove_all(backup_dir);

  BackupConfig config;
  config.backup_dir = backup_dir.string();
  config.compress = false;
  config.encrypt = true;
  config.encryption_key = "test-key";

  ClusterBackup backup;

  EXPECT_FALSE(backup.Initialize(config, nullptr));

  std::filesystem::remove_all(backup_dir);
}

TEST(ClusterBackupTest, RestoreKnownComponentFailsClosedUntilClientRestoreIsImplemented) {
  auto backup_dir = std::filesystem::temp_directory_path() / "cedar_cluster_backup_restore_unknown";
  std::filesystem::remove_all(backup_dir);

  BackupConfig config;
  config.backup_dir = backup_dir.string();
  config.compress = false;

  ClusterBackup backup;
  ASSERT_TRUE(backup.Initialize(config, nullptr));

  auto info = backup.CreateBackup("graphd", BackupType::FULL);
  std::filesystem::create_directories(info.backup_path);
  std::ofstream(backup_dir / info.backup_id / "marker").put('x');

  RestoreOptions options;
  options.backup_id = info.backup_id;
  options.verify = true;

  EXPECT_FALSE(backup.RestoreBackup(options));

  auto restored_info = backup.GetBackupInfo(info.backup_id);
  EXPECT_EQ(restored_info.status, BackupStatus::FAILED);
  EXPECT_NE(restored_info.error_message.find("not implemented"),
            std::string::npos);

  std::filesystem::remove_all(backup_dir);
}

TEST(ClusterBackupTest, CallbackCanQueryBackupManagerWithoutDeadlock) {
  auto backup_dir = std::filesystem::temp_directory_path() / "cedar_cluster_backup_callback";
  std::filesystem::remove_all(backup_dir);

  BackupConfig config;
  config.backup_dir = backup_dir.string();
  config.compress = false;

  ClusterBackup backup;
  ASSERT_TRUE(backup.Initialize(config, nullptr));

  std::atomic<int> callback_count{0};
  backup.SetCallback([&](const BackupInfo&) {
    EXPECT_GE(backup.GetTotalBackups(), 1);
    callback_count.fetch_add(1, std::memory_order_relaxed);
  });

  auto info = backup.CreateBackup("graphd", BackupType::FULL);

  EXPECT_EQ(info.status, BackupStatus::FAILED);
  EXPECT_GT(callback_count.load(std::memory_order_relaxed), 0);

  std::filesystem::remove_all(backup_dir);
}

TEST(ClusterTypesTest, DefaultConstructedStatusObjectsAreDeterministic) {
  ClusterStatusInfo status;
  EXPECT_EQ(status.status, ClusterStatus::UNKNOWN);
  EXPECT_EQ(status.metad_nodes, 0);
  EXPECT_EQ(status.storaged_healthy, 0);

  NodeInfo node;
  EXPECT_EQ(node.status, NodeStatus::UNKNOWN);
  EXPECT_EQ(node.port, 0);

  ResourceStatus resource;
  EXPECT_EQ(resource.ready_replicas, 0);
  EXPECT_EQ(resource.desired_replicas, 0);

  BackupInfo backup;
  EXPECT_EQ(backup.type, BackupType::FULL);
  EXPECT_EQ(backup.status, BackupStatus::PENDING);
  EXPECT_EQ(backup.start_time, 0);
  EXPECT_EQ(backup.end_time, 0);
  EXPECT_EQ(backup.size_bytes, 0);

  ScalingRule rule;
  EXPECT_EQ(rule.policy, ScalingPolicy::CPU_BASED);
  EXPECT_EQ(rule.min_replicas, 0);
  EXPECT_EQ(rule.max_replicas, 0);

  ScalingEvent scaling_event;
  EXPECT_EQ(scaling_event.old_replicas, 0);
  EXPECT_EQ(scaling_event.new_replicas, 0);
  EXPECT_EQ(scaling_event.timestamp, 0);

  ClusterMetrics metrics;
  EXPECT_EQ(metrics.cpu_usage_avg, 0.0);
  EXPECT_EQ(metrics.total_nodes, 0);

  Alert alert;
  EXPECT_EQ(alert.value, 0.0);
  EXPECT_EQ(alert.threshold, 0.0);
  EXPECT_EQ(alert.triggered_at, 0);
  EXPECT_FALSE(alert.resolved);
}

TEST(AutoScalerTest, RejectsNonPositivePollingIntervalAndEventLimit) {
  AutoScaler scaler;
  ASSERT_TRUE(scaler.Initialize(nullptr, nullptr));

  EXPECT_FALSE(scaler.Start(0));
  EXPECT_FALSE(scaler.Start(-1));
  EXPECT_TRUE(scaler.GetEvents(0).empty());
  EXPECT_TRUE(scaler.GetEvents(-1).empty());
}

TEST(AutoScalerTest, StopWakesPollingThreadPromptly) {
  AutoScaler scaler;
  ASSERT_TRUE(scaler.Initialize(nullptr, nullptr));
  ASSERT_TRUE(scaler.Start(60));

  auto start = std::chrono::steady_clock::now();
  scaler.Stop();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

TEST(ClusterMonitorTest, RejectsNonPositivePollingInterval) {
  ClusterMonitor monitor;
  ASSERT_TRUE(monitor.Initialize());

  EXPECT_FALSE(monitor.Start(0));
  EXPECT_FALSE(monitor.Start(-1));
}

TEST(ClusterMonitorTest, StopWakesPollingThreadPromptly) {
  ClusterMonitor monitor;
  ASSERT_TRUE(monitor.Initialize());
  ASSERT_TRUE(monitor.Start(60));

  auto start = std::chrono::steady_clock::now();
  monitor.Stop();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
}

TEST(ClusterMonitorTest, UnimplementedIntegrationsFailClosed) {
  ClusterMonitor monitor;
  ASSERT_TRUE(monitor.Initialize());

  EXPECT_FALSE(monitor.CreateGrafanaDashboard("{}"));
  EXPECT_TRUE(std::isnan(monitor.QueryPrometheus("up")));
}

TEST(DockerComposeManagerTest, RejectsUnsafeServiceNamesBeforeShellingOut) {
  DockerComposeManager manager;
  ASSERT_TRUE(manager.Initialize("compose file with spaces.yml"));

  EXPECT_FALSE(manager.StartService("metad;touch /tmp/cedar_bad"));
  EXPECT_FALSE(manager.StopService("../metad"));
  EXPECT_FALSE(manager.Scale("storaged", -1));
  EXPECT_TRUE(manager.Logs("graphd;bad", 100).empty());
  EXPECT_TRUE(manager.Logs("graphd", -1).empty());
  EXPECT_FALSE(manager.IsServiceRunning(""));
  EXPECT_EQ(manager.GetServiceReplicas(""), 0);
}

TEST(DockerComposeManagerTest, FailedComposeCommandReturnsFalse) {
  DockerComposeManager manager;
  ASSERT_TRUE(manager.Initialize("/tmp/cedar_missing_compose_file.yml"));

  EXPECT_FALSE(manager.Up(false));
}

TEST(K8sManagerTest, RejectsUnsafeResourceNamesBeforeShellingOut) {
  K8sManager manager;
  EXPECT_FALSE(manager.Initialize("bad namespace"));
  ASSERT_TRUE(manager.Initialize("cedargraph"));

  EXPECT_FALSE(manager.DeletePod("pod;bad"));
  EXPECT_FALSE(manager.ScaleDeployment("graphd", -1));
  EXPECT_FALSE(manager.RolloutRestart("../graphd"));
  EXPECT_TRUE(manager.GetPodLogs("graphd;bad", 20).empty());
  EXPECT_TRUE(manager.GetDeploymentLogs("graphd", -1).empty());
  EXPECT_TRUE(manager.GetEvents(0).empty());
}

TEST(K8sManagerTest, FailedKubectlCommandReturnsFalse) {
  K8sManager manager;
  ASSERT_TRUE(manager.Initialize("cedargraph"));

  EXPECT_FALSE(manager.Apply("/tmp/cedar_missing_k8s_manifest.yaml"));
}

TEST(K8sManagerTest, EmptyPodListIsNotReady) {
  K8sManager manager;
  ASSERT_TRUE(manager.Initialize("cedargraph-namespace-that-should-not-exist"));

  EXPECT_FALSE(manager.IsReady());
  EXPECT_EQ(manager.GetReadyPods(), 0);
}

TEST(ClusterManagerTest, RejectsUnsafeComponentOperationsBeforeShellingOut) {
  ClusterConfig config;
  config.mode = DeploymentMode::DOCKER_COMPOSE;
  config.config_file_path = "compose file with spaces.yml";

  ClusterManager manager;
  ASSERT_TRUE(manager.Initialize(config));

  EXPECT_FALSE(manager.StartComponent("graphd;bad"));
  EXPECT_FALSE(manager.StopComponent("../graphd"));
  EXPECT_FALSE(manager.RestartComponent("metad bad"));
  EXPECT_FALSE(manager.ScaleComponent("storaged", -1));
  EXPECT_TRUE(manager.GetLogs("queryd;bad", 10).empty());
  EXPECT_TRUE(manager.GetNodeLogs("cedar-graphd-1;bad", 10).empty());
  EXPECT_TRUE(manager.GetEvents(-1).empty());
}

TEST(ClusterManagerTest, FailedLifecycleCommandReturnsFalse) {
  ClusterConfig config;
  config.mode = DeploymentMode::DOCKER_COMPOSE;
  config.config_file_path = "/tmp/cedar_missing_cluster_compose.yml";

  ClusterManager manager;
  ASSERT_TRUE(manager.Initialize(config));

  EXPECT_FALSE(manager.StartCluster());
}

TEST(ClusterManagerTest, EventCallbackCanQueryEventsWithoutDeadlock) {
  ClusterConfig config;
  config.mode = DeploymentMode::DOCKER_COMPOSE;
  config.config_file_path = "/tmp/cedar_missing_cluster_callback.yml";

  ClusterManager manager;
  ASSERT_TRUE(manager.Initialize(config));

  std::atomic<int> callback_count{0};
  manager.SetEventCallback([&](const ClusterEvent&) {
    EXPECT_FALSE(manager.GetEvents(10).empty());
    callback_count.fetch_add(1, std::memory_order_relaxed);
  });

  EXPECT_FALSE(manager.StartCluster());
  EXPECT_GT(callback_count.load(std::memory_order_relaxed), 0);
}

TEST(ConfigLoaderTest, LoadFromStringReplacesPreviousConfig) {
  ConfigLoader loader;
  ASSERT_TRUE(loader.LoadFromString("[client]\nhost=old\nport=1\n[stale]\nvalue=yes\n"));
  ASSERT_TRUE(loader.HasKey("stale", "value"));

  ASSERT_TRUE(loader.LoadFromString("[client]\nhost=new\n"));

  EXPECT_EQ(loader.GetString("client", "host"), "new");
  EXPECT_EQ(loader.GetInt("client", "port", 9669), 9669);
  EXPECT_FALSE(loader.HasKey("stale", "value"));
}

TEST(ConfigWatcherTest, CallbackRunsAfterReloadWithoutHoldingWatcherLock) {
  auto config_path = std::filesystem::temp_directory_path() / "cedar_config_watcher_test.ini";
  {
    std::ofstream out(config_path);
    out << "[client]\nhost=old\n";
  }

  ConfigWatcherConfig config;
  config.file_path = config_path.string();
  config.poll_interval_ms = 10;

  std::atomic<bool> callback_called{false};
  ConfigWatcher watcher(config);
  watcher.SetChangeCallback([&](const ConfigLoader& loader) {
    callback_called = loader.GetString("client", "host") == "new";
  });

  ASSERT_TRUE(watcher.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  {
    std::ofstream out(config_path, std::ios::trunc);
    out << "[client]\nhost=new\n";
  }

  for (int i = 0; i < 100 && !callback_called.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  watcher.Stop();
  EXPECT_TRUE(callback_called.load());
  std::filesystem::remove(config_path);
}

TEST(ConfigWatcherTest, StopWakesPollingThreadPromptly) {
  auto config_path = std::filesystem::temp_directory_path() / "cedar_config_watcher_stop.ini";
  {
    std::ofstream out(config_path);
    out << "[client]\nhost=old\n";
  }

  ConfigWatcherConfig config;
  config.file_path = config_path.string();
  config.poll_interval_ms = 60000;

  ConfigWatcher watcher(config);
  ASSERT_TRUE(watcher.Start());

  auto start = std::chrono::steady_clock::now();
  watcher.Stop();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            1000);
  std::filesystem::remove(config_path);
}

TEST(JWTManagerTest, ValidateRejectsExpiredAndMalformedTokens) {
  JWTConfig config;
  config.secret_key = "test-secret";
  config.expiration_seconds = -1;
  JWTManager manager(config);

  auto expired = manager.GenerateToken("u1", "Ada");
  EXPECT_FALSE(manager.ValidateToken(expired.token));
  EXPECT_FALSE(manager.GetTokenInfo(expired.token).is_valid);
  EXPECT_FALSE(manager.ValidateToken(expired.token + ".extra"));
  EXPECT_FALSE(manager.ValidateToken("not-a-jwt"));
}

TEST(JWTManagerTest, TokenInfoParsesGeneratedPayload) {
  JWTConfig config;
  config.secret_key = "test-secret";
  config.expiration_seconds = 3600;
  JWTManager manager(config);

  auto token = manager.GenerateToken("u42", "Grace");
  auto info = manager.GetTokenInfo(token.token);

  ASSERT_TRUE(info.is_valid);
  EXPECT_EQ(info.user_id, "u42");
  EXPECT_EQ(info.user_name, "Grace");
  EXPECT_EQ(info.issuer, "cedar-client");
  EXPECT_EQ(info.audience, "cedargraph");
  EXPECT_GT(info.expires_at, info.issued_at);
}

TEST(JWTManagerTest, ValidateRejectsPayloadTampering) {
  JWTConfig config;
  config.secret_key = "test-secret";
  config.expiration_seconds = 3600;
  JWTManager manager(config);

  auto token = manager.GenerateToken("u42", "Grace");
  std::string tampered = token.token;
  size_t first_dot = tampered.find('.');
  size_t second_dot = tampered.find('.', first_dot + 1);
  ASSERT_NE(first_dot, std::string::npos);
  ASSERT_NE(second_dot, std::string::npos);
  tampered[first_dot + 1] = tampered[first_dot + 1] == 'A' ? 'B' : 'A';

  EXPECT_FALSE(manager.ValidateToken(tampered));
}

TEST(JWTManagerTest, ValidateRejectsWrongIssuerOrAudience) {
  JWTConfig config;
  config.secret_key = "test-secret";
  config.issuer = "cedar-client";
  config.audience = "cedargraph";
  JWTManager manager(config);

  const std::string header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
  auto wrong_issuer = BuildSignedJwtForTest(config, header, "other-client", "cedargraph");
  auto wrong_audience = BuildSignedJwtForTest(config, header, "cedar-client", "other-service");

  EXPECT_FALSE(manager.ValidateToken(wrong_issuer));
  EXPECT_FALSE(manager.ValidateToken(wrong_audience));
}

TEST(JWTManagerTest, ValidateRejectsUnsupportedHeaderAlgorithm) {
  JWTConfig config;
  config.secret_key = "test-secret";
  JWTManager manager(config);

  auto token = BuildSignedJwtForTest(config,
                                     "{\"alg\":\"none\",\"typ\":\"JWT\"}",
                                     "cedar-client",
                                     "cedargraph");

  EXPECT_FALSE(manager.ValidateToken(token));
}

TEST(JWTManagerTest, EmptySecretFailsClosed) {
  JWTConfig config;
  config.secret_key = "";
  JWTManager manager(config);

  auto token = manager.GenerateToken("u42", "Grace");

  EXPECT_FALSE(token.is_valid);
  EXPECT_TRUE(token.token.empty());
  EXPECT_FALSE(manager.ValidateToken(token.token));
}

TEST(CedarClientTest, JwtEnabledRequiresSecret) {
  CedarClientConfig config;
  config.enable_jwt = true;
  config.jwt_secret_key = "";
  config.enable_logging = false;
  config.enable_service_discovery = false;
  config.enable_config_watcher = false;

  CedarClient client(config);

  EXPECT_FALSE(client.Initialize());
}

TEST(ServiceDiscoveryTest, RefreshTracksDefaultNodeHealth) {
  ServiceDiscoveryConfig config;
  config.metad_host = "localhost";
  config.metad_port = 10559;
  config.heartbeat_timeout_ms = 1000;

  ServiceDiscovery discovery(config);
  ASSERT_TRUE(discovery.RefreshNodes());

  EXPECT_TRUE(discovery.IsNodeHealthy("graphd-1"));
  EXPECT_TRUE(discovery.IsNodeHealthy("storaged-1"));
  EXPECT_FALSE(discovery.IsNodeHealthy("missing-node"));
}

TEST(ServiceDiscoveryTest, HeartbeatTimeoutUsesMilliseconds) {
  ServiceDiscoveryConfig config;
  config.metad_host = "localhost";
  config.metad_port = 10559;
  config.heartbeat_timeout_ms = 1;

  ServiceDiscovery discovery(config);
  ASSERT_TRUE(discovery.RefreshNodes());
  std::this_thread::sleep_for(std::chrono::milliseconds(3));

  EXPECT_FALSE(discovery.IsNodeHealthy("graphd-1"));
}
