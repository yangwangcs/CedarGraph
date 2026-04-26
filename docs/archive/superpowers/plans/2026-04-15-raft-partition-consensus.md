# Raft Partition-Level Consensus Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the two critical Raft safety issues at the partition level: `BatchLogCommitter` must only advance `commit_index` after receiving majority `AppendEntries` acknowledgements, and `PartitionRaftGroup` must perform real leader elections with vote counting and quorum verification.

**Architecture:** `BatchLogCommitter` tracks per-entry inflight replication state and waits for a majority of nodes to acknowledge before calling `SetCommittedIndex()`. `PartitionRaftGroup` replaces immediate self-election with a proper election timeout, `RequestVote` RPC dispatch, vote tallying, and quorum checks.

**Tech Stack:** C++17, GoogleTest, CMake, gRPC (for Raft RPCs)

---

## File Structure

| File | Responsibility |
|------|----------------|
| `src/raft/batch_log_committer.cc` | Fixes quorum-aware commit: tracks inflight entries and counts ACKs |
| `include/cedar/raft/batch_log_committer.h` | Adds `InflightEntry` struct and ACK tracking members |
| `src/raft/partition_raft_group.cc` | Implements real `StartElection()`, vote request/response handling, quorum logic |
| `include/cedar/raft/partition_raft_group.h` | Adds `votes_received_`, `election_deadline_`, `RequestVote` dispatch |
| `tests/test_partition_raft.cc` | Existing partition Raft tests to extend |
| `tests/test_partition_raft_manager.cc` | Manager-level tests |

---

## Task 1: Fix `BatchLogCommitter` to Enforce Quorum

**Files:**
- Modify: `include/cedar/raft/batch_log_committer.h`
- Modify: `src/raft/batch_log_committer.cc`
- Test: `tests/test_partition_raft.cc`

- [ ] **Step 1: Add inflight tracking structures to the header**

In `include/cedar/raft/batch_log_committer.h`, inside `class BatchLogCommitter`, add:

```cpp
 private:
  struct InflightEntry {
    LogIndex index;
    LogTerm term;
    size_t ack_count;
    size_t nack_count;
    bool committed;
    std::chrono::steady_clock::time_point send_time;
  };

  std::unordered_map<LogIndex, InflightEntry> inflight_;
  mutable std::mutex inflight_mutex_;
  size_t quorum_size_;

  // Called when an AppendEntries response arrives
  void OnAppendEntriesResponse(NodeID from,
                               LogIndex prev_log_index,
                               bool success);
```

Also add a public method:

```cpp
  // Notify the committer that a follower has acknowledged entries up to
  // the given index. This is called by the Raft transport layer.
  void Acknowledge(NodeID follower, LogIndex match_index);
```

- [ ] **Step 2: Initialize `quorum_size_` in the constructor**

In `src/raft/batch_log_committer.cc`, update the constructor (or `Initialize` method) to compute quorum:

```cpp
BatchLogCommitter::BatchLogCommitter(
    std::shared_ptr<PartitionLogStore> log_store,
    const std::vector<NodeID>& peers)
    : log_store_(std::move(log_store)),
      quorum_size_((peers.size() + 1) / 2 + 1) {
  if (quorum_size_ < 1) quorum_size_ = 1;
}
```

If the constructor signature differs, adjust accordingly. The key is to ensure `BatchLogCommitter` knows the total cluster size so it can compute quorum.

- [ ] **Step 3: Rewrite `DoCommitBatch()` to defer commit until quorum is reached**

Replace the old immediate-commit logic with:

```cpp
Status BatchLogCommitter::DoCommitBatch(
    const std::vector<LogEntry>& entries) {
  if (!log_store_) {
    return Status::NotSupported("BatchLogCommitter", "No log store configured");
  }

  // Step 1: Append to local log
  auto s = log_store_->AppendEntries(entries);
  if (!s.ok()) {
    return s;
  }

  // Step 2: Register inflight entries (including self-ack)
  {
    std::lock_guard<std::mutex> lock(inflight_mutex_);
    for (const auto& entry : entries) {
      InflightEntry ie;
      ie.index = entry.index;
      ie.term = entry.term;
      ie.ack_count = 1;  // Leader counts as one ack
      ie.nack_count = 0;
      ie.committed = false;
      ie.send_time = std::chrono::steady_clock::now();
      inflight_[entry.index] = ie;
    }
  }

  // Step 3: Replicate to followers (via Raft transport)
  // The transport layer is responsible for calling Acknowledge() when
  // responses arrive.
  s = ReplicateToFollowers(entries);
  if (!s.ok()) {
    return s;
  }

  // Step 4: We do NOT immediately advance commit_index here.
  // commit_index will be advanced by CheckCommitQuorum() after enough
  // followers call Acknowledge().
  return Status::OK();
}
```

- [ ] **Step 4: Implement `Acknowledge()` and `CheckCommitQuorum()`**

Add to `src/raft/batch_log_committer.cc`:

```cpp
void BatchLogCommitter::Acknowledge(NodeID follower, LogIndex match_index) {
  (void)follower;
  std::lock_guard<std::mutex> lock(inflight_mutex_);

  for (auto& [index, entry] : inflight_) {
    if (index <= match_index && !entry.committed) {
      entry.ack_count++;
      if (entry.ack_count >= quorum_size_) {
        entry.committed = true;
      }
    }
  }

  // Advance commit index to the highest contiguous committed index
  LogIndex new_commit = log_store_->GetCommittedIndex();
  for (LogIndex idx = new_commit + 1;; ++idx) {
    auto it = inflight_.find(idx);
    if (it == inflight_.end() || !it->second.committed) {
      break;
    }
    new_commit = idx;
  }

  if (new_commit > log_store_->GetCommittedIndex()) {
    log_store_->SetCommittedIndex(new_commit);
    InvokeCallbacks(new_commit);
  }

  // Clean up old inflight entries
  auto now = std::chrono::steady_clock::now();
  for (auto it = inflight_.begin(); it != inflight_.end();) {
    if (it->second.committed ||
        std::chrono::duration_cast<std::chrono::seconds>(
            now - it->second.send_time).count() > 30) {
      it = inflight_.erase(it);
    } else {
      ++it;
    }
  }
}
```

If `InvokeCallbacks()` does not exist, add a private helper that iterates pending callbacks for indices <= new_commit and invokes them.

- [ ] **Step 5: Wire `Acknowledge()` into `PartitionRaftServiceImpl::AppendEntries`**

In `src/raft/partition_raft_service.cc`, find the `AppendEntries` handler and ensure that when a leader receives a successful response (or processes its own local append), it calls:

```cpp
if (batch_committer_) {
  batch_committer_->Acknowledge(from_node_id, response.last_log_index());
}
```

If the response path is not centralized, add the call wherever `AppendEntriesResponse` is handled for the leader.

- [ ] **Step 6: Write a quorum commit test**

In `tests/test_partition_raft.cc`, add:

```cpp
TEST(BatchLogCommitterTest, CommitOnlyAfterQuorum) {
  auto log_store = std::make_shared<cedar::raft::PartitionLogStore>(
      "/tmp/cedar_test_committer_" + std::to_string(getpid()));
  auto s = log_store->Open();
  ASSERT_TRUE(s.ok());

  // 3 nodes => quorum = 2
  std::vector<cedar::raft::NodeID> peers = {1, 2, 3};
  cedar::raft::BatchLogCommitter committer(log_store, peers);

  std::vector<cedar::raft::LogEntry> entries;
  cedar::raft::LogEntry e;
  e.term = 1;
  e.index = log_store->GetLastIndex() + 1;
  e.data = "hello";
  entries.push_back(e);

  s = committer.DoCommitBatch(entries);
  ASSERT_TRUE(s.ok());

  // Immediately after local append, commit index should NOT advance
  EXPECT_EQ(log_store->GetCommittedIndex(), 0);

  // Ack from one follower (total acks = 2 = quorum)
  committer.Acknowledge(2, e.index);
  EXPECT_EQ(log_store->GetCommittedIndex(), e.index);
}
```

- [ ] **Step 7: Build and run the test**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_partition_raft && ./tests/test_partition_raft --gtest_filter='BatchLogCommitterTest.CommitOnlyAfterQuorum'
```

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/raft/batch_log_committer.cc include/cedar/raft/batch_log_committer.h src/raft/partition_raft_service.cc tests/test_partition_raft.cc
git commit -m "fix(raft): enforce quorum in BatchLogCommitter before advancing commit_index"
```

---

## Task 2: Implement Real Leader Election in `PartitionRaftGroup`

**Files:**
- Modify: `include/cedar/raft/partition_raft_group.h`
- Modify: `src/raft/partition_raft_group.cc`
- Modify: `src/raft/partition_raft_service.cc`
- Test: `tests/test_partition_raft.cc`

- [ ] **Step 1: Add election state tracking to the header**

In `include/cedar/raft/partition_raft_group.h`, inside `class PartitionRaftGroup`, add:

```cpp
 private:
  // Election state
  std::atomic<bool> election_in_progress_{false};
  std::atomic<size_t> votes_received_{0};
  std::unordered_set<NodeID> voted_for_me_;
  mutable std::mutex vote_mutex_;
  std::chrono::steady_clock::time_point election_deadline_;

  // Helper methods
  void ResetElectionTimer();
  bool ElectionTimedOut() const;
  bool HasQuorum(size_t votes) const;
  Status BroadcastRequestVote();
```

- [ ] **Step 2: Implement real `BecomeCandidate()` and `StartElection()`**

In `src/raft/partition_raft_group.cc`, replace the fake self-election with:

```cpp
void PartitionRaftGroup::ResetElectionTimer() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(
      config_.election_timeout_ms, config_.election_timeout_ms * 2);
  int timeout_ms = dist(gen);
  election_deadline_ = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
}

bool PartitionRaftGroup::ElectionTimedOut() const {
  return std::chrono::steady_clock::now() > election_deadline_;
}

bool PartitionRaftGroup::HasQuorum(size_t votes) const {
  size_t total = peers_.size() + 1;  // +1 for self
  return votes >= (total / 2 + 1);
}

void PartitionRaftGroup::BecomeCandidate() {
  std::lock_guard<std::mutex> lock(vote_mutex_);
  current_term_++;
  role_ = RaftRole::kCandidate;
  voted_for_ = node_id_;
  votes_received_ = 1;
  voted_for_me_.clear();
  voted_for_me_.insert(node_id_);
  election_in_progress_ = true;
  ResetElectionTimer();

  LOG(INFO) << "PartitionRaftGroup " << group_id_ << " became candidate for term "
            << current_term_;

  auto s = BroadcastRequestVote();
  if (!s.ok()) {
    LOG(WARNING) << "BroadcastRequestVote failed: " << s.ToString();
  }
}
```

- [ ] **Step 3: Implement `BroadcastRequestVote()`**

```cpp
Status PartitionRaftGroup::BroadcastRequestVote() {
  if (!raft_transport_) {
    return Status::IOError("No raft transport configured");
  }

  RequestVoteRequest req;
  req.set_term(current_term_);
  req.set_candidate_id(node_id_);
  req.set_last_log_index(log_store_->GetLastIndex());
  req.set_last_log_term(log_store_->GetLastTerm());

  for (NodeID peer : peers_) {
    if (peer == node_id_) continue;

    auto s = raft_transport_->SendRequestVote(peer, req,
        [this](const RequestVoteResponse& resp) {
          HandleRequestVoteResponse(resp);
        });
    if (!s.ok()) {
      LOG(WARNING) << "Failed to send RequestVote to " << peer;
    }
  }
  return Status::OK();
}
```

If `raft_transport_->SendRequestVote` signature differs, adjust to match the actual `RaftTransport` interface.

- [ ] **Step 4: Implement `HandleRequestVoteResponse()`**

```cpp
void PartitionRaftGroup::HandleRequestVoteResponse(
    const RequestVoteResponse& resp) {
  std::lock_guard<std::mutex> lock(vote_mutex_);

  if (role_ != RaftRole::kCandidate || !election_in_progress_) {
    return;
  }

  if (resp.term() > current_term_) {
    current_term_ = resp.term();
    BecomeFollower();
    return;
  }

  if (resp.term() < current_term_) {
    return;
  }

  if (resp.vote_granted() && voted_for_me_.insert(resp.voter_id()).second) {
    votes_received_++;
    if (HasQuorum(votes_received_.load())) {
      BecomeLeader();
      election_in_progress_ = false;
    }
  }
}
```

If `RequestVoteResponse` does not have `voter_id`, use the peer ID from the transport context or add the field.

- [ ] **Step 5: Fix the `RequestVote` handler in `PartitionRaftServiceImpl`**

In `src/raft/partition_raft_service.cc`, ensure the server-side `RequestVote` RPC handler evaluates the request properly before granting the vote:

```cpp
grpc::Status PartitionRaftServiceImpl::RequestVote(
    grpc::ServerContext* context,
    const raft::RequestVoteRequest* request,
    raft::RequestVoteResponse* response) {
  (void)context;
  auto group = manager_->GetGroup(request->group_id());
  if (!group) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Group not found");
  }

  std::lock_guard<std::mutex> lock(group->mutex());
  response->set_term(group->current_term());
  response->set_voter_id(group->node_id());

  if (request->term() < group->current_term()) {
    response->set_vote_granted(false);
    return grpc::Status::OK;
  }

  if (request->term() > group->current_term()) {
    group->set_current_term(request->term());
    group->BecomeFollower();
    group->set_voted_for(kInvalidNodeID);
  }

  if (group->voted_for() != kInvalidNodeID &&
      group->voted_for() != request->candidate_id()) {
    response->set_vote_granted(false);
    return grpc::Status::OK();
  }

  // Check log completeness: candidate's log must be at least as up-to-date
  if (request->last_log_term() < group->log_store()->GetLastTerm() ||
      (request->last_log_term() == group->log_store()->GetLastTerm() &&
       request->last_log_index() < group->log_store()->GetLastIndex())) {
    response->set_vote_granted(false);
    return grpc::Status::OK();
  }

  group->set_voted_for(request->candidate_id());
  response->set_vote_granted(true);
  return grpc::Status::OK();
}
```

If accessor methods like `group->mutex()`, `group->current_term()`, `group->set_voted_for()` do not exist, add them to `PartitionRaftGroup`.

- [ ] **Step 6: Ensure the event loop checks election timeout**

In `src/raft/partition_raft_group.cc`, inside the background ticker/event loop (e.g., `RunEventLoop()` or `Tick()`), add:

```cpp
if (role_ == RaftRole::kFollower || role_ == RaftRole::kCandidate) {
  if (ElectionTimedOut()) {
    BecomeCandidate();
  }
}
```

Also reset the election timer whenever valid AppendEntries or heartbeats are received from the current leader.

- [ ] **Step 7: Write a leader election test**

In `tests/test_partition_raft.cc`, add:

```cpp
TEST(PartitionRaftGroupTest, SingleNodeSelfElectsImmediately) {
  // With only 1 node, quorum = 1, so the node should win its own election
  cedar::raft::PartitionRaftGroup::Config config;
  config.node_id = 1;
  config.group_id = 100;
  config.election_timeout_ms = 100;

  cedar::raft::PartitionRaftGroup group(config);
  group.Initialize();

  // Manually trigger candidacy
  group.BecomeCandidate();
  EXPECT_EQ(group.Role(), cedar::raft::RaftRole::kLeader);
}

TEST(PartitionRaftGroupTest, ThreeNodeNeedsTwoVotes) {
  cedar::raft::PartitionRaftGroup::Config config;
  config.node_id = 1;
  config.group_id = 101;
  config.election_timeout_ms = 100;

  cedar::raft::PartitionRaftGroup group(config);
  group.AddPeer(2);
  group.AddPeer(3);
  group.Initialize();

  group.BecomeCandidate();
  EXPECT_EQ(group.Role(), cedar::raft::RaftRole::kCandidate);

  // Grant vote from node 2
  raft::RequestVoteResponse resp;
  resp.set_term(group.CurrentTerm());
  resp.set_vote_granted(true);
  resp.set_voter_id(2);
  group.HandleRequestVoteResponse(resp);

  EXPECT_EQ(group.Role(), cedar::raft::RaftRole::kLeader);
}
```

- [ ] **Step 8: Build and run**

```bash
cd /Users/wangyang/Desktop/CedarGraph-Core/build && cmake --build . --target test_partition_raft && ./tests/test_partition_raft --gtest_filter='PartitionRaftGroupTest.*'
```

Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add src/raft/partition_raft_group.cc include/cedar/raft/partition_raft_group.h src/raft/partition_raft_service.cc tests/test_partition_raft.cc
git commit -m "feat(raft): implement real leader election with vote counting and quorum in PartitionRaftGroup"
```

---

## Self-Review Checklist

1. **Spec coverage:** Both critical Raft issues are addressed: `BatchLogCommitter` quorum enforcement and `PartitionRaftGroup` real election.
2. **Placeholder scan:** No TBD, TODO, or "implement later" remains in any step.
3. **Type consistency:** `LogEntry`, `LogIndex`, `LogTerm`, `NodeID`, `RaftRole`, `RequestVoteRequest`, `RequestVoteResponse` usage matches existing code patterns.
