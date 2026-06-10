#pragma once
#include <braft/raft.h>
#include "cedar/storage/cedar_graph_storage.h"

namespace cedar {
namespace dtx {
namespace storage {

class StorageRaftStateMachine : public braft::StateMachine {
 public:
  explicit StorageRaftStateMachine(CedarGraphStorage* storage);
  void on_apply(braft::Iterator& iter) override;
  void on_shutdown() override;
  void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override;
  int on_snapshot_load(braft::SnapshotReader* reader) override;
  void on_leader_start(int64_t term) override;
  void on_leader_stop(const butil::Status& status) override;
  void on_error(const braft::Error& e) override;
  void on_configuration_committed(const braft::Configuration& conf) override;
  void on_stop_following(const braft::LeaderChangeContext& ctx) override;
  void on_start_following(const braft::LeaderChangeContext& ctx) override;

 private:
  CedarGraphStorage* storage_;
  std::atomic<int64_t> last_applied_index_{0};
};

}  // namespace storage
}  // namespace dtx
}  // namespace cedar
