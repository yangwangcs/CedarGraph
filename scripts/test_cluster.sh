#!/bin/bash
# Test a 3-node MetaD Raft cluster using the current cedar-metad binary.
#
# This script intentionally delegates to the maintained non-test-mode Raft smoke
# instead of the removed HTTP example binary.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "${SCRIPT_DIR}/preflight_non_test_raft_smoke.sh"
