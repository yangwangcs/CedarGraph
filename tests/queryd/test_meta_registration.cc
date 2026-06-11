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

// =============================================================================
// QueryD MetaClient Registration and Heartbeat Test
// =============================================================================
// Verifies that QueryMetaClient::RegisterQueryD and Heartbeat perform
// actual gRPC calls to the MetaService using mandatory TLS.
// =============================================================================

#include <gtest/gtest.h>

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <cstdlib>
#include <fstream>
#include <string>

#include "cedar/dtx/raft/grpc_tls.h"
#include "cedar/queryd/meta_client.h"
#include "meta_service.grpc.pb.h"
#include "meta_service.pb.h"

using namespace cedar;
using namespace cedar::queryd;

namespace {

// Self-signed test certificate (CN=localhost) used for the mock MetaD server.
// The client verifies the same certificate as its CA trust anchor.
constexpr char kTestServerCert[] = R"(-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUUUJIyphUOdkDqkvu8hbVu1TucnYwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYxMTAzMDMwNloXDTI3MDYx
MTAzMDMwNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAjvuVzd3YM7gFJZf//tD4jxmr20C9wrjdYF2ImTMg5xlF
VxEWtwkU+JREVeAbZEPn5cBEyUIQOh8K1aklTpH+PIjaFkUJEZPYWjfMaNG/yJZ9
PJIe91ptcCb3yRZgZf6jifa7lfXMwMYWlv1DsUANcynjAhfaoQPfXxUwHm5JDu9O
7kCy87m9nC0xfhTK9mtS9fdpKKA7hqda4gLtZuZbZfJQ0mwYDHWrJiY3jdjQLan7
JzK7qaGoqh2LhjiHRIdnIVSqriTJnDaHdTqWfPSz3HjsZhLhjlZEhOVpbYLTuahT
UqBE2dXyGFy8/EMpJlFjJR5cZ2Y6QYM2Fyeu8rTlxQIDAQABo1MwUTAdBgNVHQ4E
FgQUmMdSDMkAZbxoWHIOQM+L8ItOnyUwHwYDVR0jBBgwFoAUmMdSDMkAZbxoWHIO
QM+L8ItOnyUwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAauvh
fnBatzuIZ62xuIGklMJ52zRcFppnXxXPxi73Av9XnHKc7xxL9msrn+8uaYiIbuRa
I0Jhbkm4ImDv9VpNCS0AtOX0Gkm44Sur5efjyRrEKTda7ENjW2Ptl6foIhmiy0GT
tRciCDio0jJGYfFC0Xz4sBb9vTmN8T2sqNSJVW+edjfU78XMfFLl8kuUnC8huTun
Qij1sYt2h5TEs++EIK0UoJIM4IdBjt8eSJ1yKftv7thxeddUQ6Fxxmx1PCIlwCC7
Kn0+CVvpox4/xFdcoVi+obtwdi5VCo6ectxQQ0GXxei8BY9zVBWeNfFKxorJgNpL
twdnmBD2l7Lp4lvC5Q==
-----END CERTIFICATE-----
)";

constexpr char kTestServerKey[] = R"(-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCO+5XN3dgzuAUl
l//+0PiPGavbQL3CuN1gXYiZMyDnGUVXERa3CRT4lERV4BtkQ+flwETJQhA6HwrV
qSVOkf48iNoWRQkRk9haN8xo0b/Iln08kh73Wm1wJvfJFmBl/qOJ9ruV9czAxhaW
/UOxQA1zKeMCF9qhA99fFTAebkkO707uQLLzub2cLTF+FMr2a1L192kooDuGp1ri
Au1m5ltl8lDSbBgMdasmJjeN2NAtqfsnMrupoaiqHYuGOIdEh2chVKquJMmcNod1
OpZ89LPceOxmEuGOVkSE5WltgtO5qFNSoETZ1fIYXLz8QykmUWMlHlxnZjpBgzYX
J67ytOXFAgMBAAECggEALR+Lssj4wrWn9inGkdnMH4kX+d0wJcQmpRNPmR2QHC6W
+fe8Je55TkuoVzufGWDuzcyESMmPCnCigDRdwDKFu//qZ43I42G3rR0f5tKPBlQr
2NI6cJB6qiK6Hx1vNbELVm5l29kTEaFSHrt1wfn3ZKlK6W2yww7QTxcGNQxUBSCb
EFzRuNvAMxWwOH71Cr/v2Fx+j+3ql7Ayv3yrNlNERNybdOB3ox2Vy/mMfVwCg7b6
ZScHzZWPFm/dzqr6wNoAwQo6oIjCmGHlgDSSFzxnfUMqz7413GOwuufqSU1Q1tiC
wQ+l+DOz1+c6+t0ZtjXXLpVxjiAwQPBPKgZgksQIDwKBgQDCMq+9FLFuZevdUKBh
CwBUzfeCTvnRi2lKn03TG5rRFb2Iw53GWx7Rl27QNo550YYgp6ooCk0pkb0d696Y
F8KHG/WTPq4x7u3fV2v0TArHWwprbwMui3Uc2LSdpxBe1Db8r9qiXUogs3Ax1X5x
Zg1AhT09co1+B32s7NlsFECuIwKBgQC8fGRxe/fzocsU6c19IIlnRo/atwL2DSfK
cT36ZBGy0tp6BftMl5W6rLJo5T/OONIUVk+PSe6zalqrZdLHJD5vL6W8FdnI+znK
6jKkGYBm4x66qINBYJiSN8orut3X0oTrJ2fRdh67xOyCA/4WSykwxeh1iXfCFfvB
lzWqaYC29wKBgEVVH1UcXDSUAt+i939uFBIy7tkBJUPgyBiyQ3DJfD6FyoNXg67b
vWcK7686qydm3MIv2hotg1sCA0j5eyFF6leebdDCIiMFsLt6VLqFo5uFL3UnzzUA
6TEBVYqrqLaSgYc5qY8qS1rddYL1PA10Z+rPJwwXJ9kFB6ODdCSYHneNAoGAYXFJ
mCXHzQtS6w/oLQ0aG+styZudi0jHzm/247DCOZmqWzUmcrVXMffAEFycPOfBK8Rn
QyOspNKR51QvwMYrBN40J2WAftfqS84BujZ43DgElekyWiUvG0B+Y1crAz2Re+SW
VoJjZx1qS9j2jd3zgISAJeuYnx0wVyfuFZiPc4cCgYBkSMcRoj65BT+DoMeqF+vK
aTRTzD97vzS+BKMCYki+Vi/1hzkldDbojQxffVLGtCzRo+dAYXaOALmPVKP6TMbg
YH9z3YrteCiD6zC0WGahiL22sOUd7T9cnQZC/RjW1YKfoYH70rbzCwlCSsK/HM1h
6XXW6iww6TWHHzu5NLcjig==
-----END PRIVATE KEY-----
)";

static bool WriteFile(const std::string& path, const std::string& content) {
  std::ofstream f(path);
  if (!f.is_open()) return false;
  f << content;
  return f.good();
}

static std::string GetTestCertPath() {
  return "/tmp/cedar_meta_reg_test_cert.pem";
}

static std::string GetTestKeyPath() {
  return "/tmp/cedar_meta_reg_test_key.pem";
}

static void SetupTlsEnvironment() {
  ASSERT_TRUE(WriteFile(GetTestCertPath(), kTestServerCert));
  ASSERT_TRUE(WriteFile(GetTestKeyPath(), kTestServerKey));
  setenv("CEDAR_GRPC_TLS_ENABLED", "1", 1);
  setenv("CEDAR_GRPC_SERVER_CERT", GetTestCertPath().c_str(), 1);
  setenv("CEDAR_GRPC_SERVER_KEY", GetTestKeyPath().c_str(), 1);
  setenv("CEDAR_GRPC_CA_CERT", GetTestCertPath().c_str(), 1);
}

}  // namespace

class MockMetaServiceImpl : public cedar::meta::MetaService::Service {
 public:
  grpc::Status GetSpacePartitionMap(
      grpc::ServerContext* /*context*/,
      const cedar::meta::GetSpacePartitionMapRequest* /*request*/,
      cedar::meta::GetSpacePartitionMapResponse* response) override {
    response->set_success(true);
    response->mutable_partition_map()->set_space_name("default");
    response->mutable_partition_map()->set_num_partitions(1);
    return grpc::Status::OK;
  }

  grpc::Status GetAliveNodes(
      grpc::ServerContext* /*context*/,
      const cedar::meta::GetAliveNodesRequest* /*request*/,
      cedar::meta::GetAliveNodesResponse* response) override {
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status RegisterQueryD(
      grpc::ServerContext* /*context*/,
      const cedar::meta::RegisterQueryDRequest* request,
      cedar::meta::RegisterQueryDResponse* response) override {
    last_listen_address = request->listen_address();
    response->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status QueryDHeartbeat(
      grpc::ServerContext* /*context*/,
      const cedar::meta::QueryDHeartbeatRequest* request,
      cedar::meta::QueryDHeartbeatResponse* response) override {
    last_active_queries = request->active_queries();
    last_queued_queries = request->queued_queries();
    response->set_success(true);
    return grpc::Status::OK;
  }

  std::string last_listen_address;
  uint32_t last_active_queries = 0;
  uint32_t last_queued_queries = 0;
};

class MetaRegistrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SetupTlsEnvironment();

    auto creds = cedar::dtx::raft::TlsCredentialFactory::CreateServerCredentialsFromEnv();
    ASSERT_TRUE(creds.ok()) << creds.status().ToString();

    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", creds.ValueOrDie(), &port_);
    builder.RegisterService(&mock_service_);
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    ASSERT_GT(port_, 0);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
  }

  std::string GetServerAddress() const {
    // Connect by hostname so the self-signed cert's CN (localhost) verifies.
    return "localhost:" + std::to_string(port_);
  }

  MockMetaServiceImpl mock_service_;
  std::unique_ptr<grpc::Server> server_;
  int port_ = 0;
};

TEST_F(MetaRegistrationTest, RegisterQueryDSendsAddress) {
  QueryMetaClient::Options options;
  options.meta_service_address = GetServerAddress();
  options.rpc_timeout = std::chrono::milliseconds(5000);
  options.enable_cache = false;
  options.refresh_interval = std::chrono::seconds(1);

  QueryMetaClient client(options);
  Status s = client.Init();
  EXPECT_TRUE(s.ok()) << s.ToString();

  s = client.RegisterQueryD("127.0.0.1:9669");
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(mock_service_.last_listen_address, "127.0.0.1:9669");
}

TEST_F(MetaRegistrationTest, HeartbeatSendsQueryCounts) {
  QueryMetaClient::Options options;
  options.meta_service_address = GetServerAddress();
  options.rpc_timeout = std::chrono::milliseconds(5000);
  options.enable_cache = false;
  options.refresh_interval = std::chrono::seconds(1);

  QueryMetaClient client(options);
  Status s = client.Init();
  EXPECT_TRUE(s.ok()) << s.ToString();

  s = client.Heartbeat(5, 3);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(mock_service_.last_active_queries, 5u);
  EXPECT_EQ(mock_service_.last_queued_queries, 3u);
}

TEST_F(MetaRegistrationTest, HeartbeatZeroQueries) {
  QueryMetaClient::Options options;
  options.meta_service_address = GetServerAddress();
  options.rpc_timeout = std::chrono::milliseconds(5000);
  options.enable_cache = false;
  options.refresh_interval = std::chrono::seconds(1);

  QueryMetaClient client(options);
  Status s = client.Init();
  EXPECT_TRUE(s.ok()) << s.ToString();

  s = client.Heartbeat(0, 0);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(mock_service_.last_active_queries, 0u);
  EXPECT_EQ(mock_service_.last_queued_queries, 0u);
}
