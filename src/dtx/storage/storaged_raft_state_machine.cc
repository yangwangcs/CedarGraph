#include "cedar/dtx/storage/storaged_raft_state_machine.h"
#include <brpc/closure_guard.h>
#include <butil/logging.h>

namespace cedar { namespace dtx { namespace storage {

StorageRaftStateMachine::StorageRaftStateMachine(CedarGraphStorage* storage)
    : storage_(storage) {}

void StorageRaftStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    brpc::ClosureGuard done_guard(iter.done());
    LOG(INFO) << "Raft apply: index=" << iter.index() << " term=" << iter.term();
    // TODO: deserialize iter.data() and apply to storage_
  }
}

void StorageRaftStateMachine::on_shutdown() {}

void StorageRaftStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                                                braft::Closure* done) {
  LOG(INFO) << "Raft snapshot save";
  if (done) done->Run();
}

int StorageRaftStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
  LOG(INFO) << "Raft snapshot load";
  return 0;
}

void StorageRaftStateMachine::on_leader_start(int64_t term) {
  LOG(INFO) << "Raft leader started, term=" << term;
}

void StorageRaftStateMachine::on_leader_stop(const butil::Status& status) {
  LOG(INFO) << "Raft leader stopped: " << status.error_str();
}

void StorageRaftStateMachine::on_error(const braft::Error& e) {
  LOG(ERROR) << "Raft error: " << e.status().error_str();
}

void StorageRaftStateMachine::on_configuration_committed(const braft::Configuration& conf) {
  LOG(INFO) << "Raft configuration committed";
}

void StorageRaftStateMachine::on_stop_following(const braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "Raft stop following leader " << ctx.leader_id();
}

void StorageRaftStateMachine::on_start_following(const braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "Raft start following leader " << ctx.leader_id();
}

}}}  // namespace cedar::dtx::storage
