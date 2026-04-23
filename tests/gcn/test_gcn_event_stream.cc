#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>
#include <limits>
#include <thread>

#include "cedar/gcn/event_applier.h"
#include "cedar/gcn/gcn_service.h"
#include "cedar/gcn/tmv_engine.h"
#include "gcn_service.grpc.pb.h"

using namespace cedar::gcn;

class GcnEventStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    engine_ = std::make_unique<TMVEngine>(16);
    applier_ = std::make_unique<EventApplier>(engine_.get());

    auto callback = [this](const cedar::gcn::CDCEvent& proto_event) {
      GraphCDCEvent event;
      event.commit_version = proto_event.version();
      event.entity_id = proto_event.entity_id();
      event.target_id = proto_event.entity_id() + 1;
      event.valid_from = static_cast<uint32_t>(proto_event.timestamp());
      event.valid_to = std::numeric_limits<uint32_t>::max();
      event.edge_type = 1;
      event.op = CDCEventOp::kCreate;
      this->applier_->ApplyUnordered(event);
    };

    service_ = std::make_unique<GcnServiceImpl>(std::move(callback));
    server_address_ = "127.0.0.1:0";

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(), &port_);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);

    std::ostringstream oss;
    oss << "127.0.0.1:" << port_;
    server_address_ = oss.str();

    server_thread_ = std::thread([this]() { server_->Wait(); });

    auto channel = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    stub_ = GcnService::NewStub(channel);
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
    }
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }

  std::unique_ptr<TMVEngine> engine_;
  std::unique_ptr<EventApplier> applier_;
  std::unique_ptr<GcnServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread server_thread_;
  std::string server_address_;
  int port_ = 0;
  std::unique_ptr<GcnService::Stub> stub_;
};

TEST_F(GcnEventStreamTest, StreamEventsToClient) {
  // Enqueue events with sequential versions so they apply in order
  for (uint64_t i = 1; i <= 3; ++i) {
    cedar::gcn::CDCEvent event;
    event.set_event_id("event-" + std::to_string(i));
    event.set_entity_id(100 + i);
    event.set_version(i);
    event.set_timestamp(1000 + i * 100);
    event.set_event_type("CREATE");
    service_->EnqueueEvent(event);
  }

  grpc::ClientContext context;
  auto stream = stub_->OnEventStream(&context);

  // Read 3 events and ack each one
  std::vector<uint64_t> received_versions;
  for (int i = 0; i < 3; ++i) {
    cedar::gcn::CDCEvent event;
    ASSERT_TRUE(stream->Read(&event));
    received_versions.push_back(event.version());

    cedar::gcn::Ack ack;
    ack.set_event_id(event.event_id());
    ack.set_accepted(true);
    ASSERT_TRUE(stream->Write(ack));
  }

  stream->WritesDone();
  grpc::Status status = stream->Finish();
  EXPECT_TRUE(status.ok());

  EXPECT_EQ(received_versions.size(), 3u);
  EXPECT_EQ(received_versions[0], 1u);
  EXPECT_EQ(received_versions[1], 2u);
  EXPECT_EQ(received_versions[2], 3u);

  // Verify EventApplier applied all events
  EXPECT_EQ(applier_->applied_version(), 3u);
}

TEST_F(GcnEventStreamTest, StreamEventsOutOfOrderVersions) {
  // Enqueue events with versions 2, 1, 3
  // ApplyUnordered should buffer 2, apply 1, then drain 2, then apply 3
  {
    cedar::gcn::CDCEvent e2;
    e2.set_event_id("event-2");
    e2.set_entity_id(200);
    e2.set_version(2);
    e2.set_timestamp(2000);
    e2.set_event_type("CREATE");
    service_->EnqueueEvent(e2);
  }
  {
    cedar::gcn::CDCEvent e1;
    e1.set_event_id("event-1");
    e1.set_entity_id(100);
    e1.set_version(1);
    e1.set_timestamp(1000);
    e1.set_event_type("CREATE");
    service_->EnqueueEvent(e1);
  }
  {
    cedar::gcn::CDCEvent e3;
    e3.set_event_id("event-3");
    e3.set_entity_id(300);
    e3.set_version(3);
    e3.set_timestamp(3000);
    e3.set_event_type("CREATE");
    service_->EnqueueEvent(e3);
  }

  grpc::ClientContext context;
  auto stream = stub_->OnEventStream(&context);

  for (int i = 0; i < 3; ++i) {
    cedar::gcn::CDCEvent event;
    ASSERT_TRUE(stream->Read(&event));

    cedar::gcn::Ack ack;
    ack.set_event_id(event.event_id());
    ack.set_accepted(true);
    ASSERT_TRUE(stream->Write(ack));
  }

  stream->WritesDone();
  grpc::Status status = stream->Finish();
  EXPECT_TRUE(status.ok());

  // All three should be applied regardless of stream order
  EXPECT_EQ(applier_->applied_version(), 3u);
}
