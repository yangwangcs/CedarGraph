#include <gtest/gtest.h>
#include "cedar/dtx/storage/storaged_raft_state_machine.h"

TEST(StorageRaftStateMachine, ConstructsWithoutCrash) {
  // Test with nullptr (state machine should handle it gracefully)
  cedar::dtx::storage::StorageRaftStateMachine sm(nullptr);
  sm.on_shutdown();
}
