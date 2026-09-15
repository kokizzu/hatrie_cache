#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --stat -- BENCHMARK.md INSPIRATION.md READ_REPLICA_SELECTION_FASTPATH.md hat/hatReplication/read_consistency.go hat/hatReplication/read_replica_policy.go hat/hatReplication/read_replica_selection_fastpath_test.go scripts/test-read-replica-policy-c213.sh scripts/benchmark-read-replica-policy-c213.sh scripts/format-read-replica-policy-c213.sh scripts/verify-read-replica-policy-c213.sh scripts/inspect-read-replica-diff-c213.sh scripts/stage-read-replica-policy-c213.sh scripts/commit-read-replica-policy-c213.sh scripts/push-read-replica-policy-c213.sh
git diff --check -- BENCHMARK.md INSPIRATION.md READ_REPLICA_SELECTION_FASTPATH.md hat/hatReplication/read_consistency.go hat/hatReplication/read_replica_policy.go hat/hatReplication/read_replica_selection_fastpath_test.go scripts/test-read-replica-policy-c213.sh scripts/benchmark-read-replica-policy-c213.sh scripts/format-read-replica-policy-c213.sh scripts/verify-read-replica-policy-c213.sh scripts/inspect-read-replica-diff-c213.sh scripts/stage-read-replica-policy-c213.sh scripts/commit-read-replica-policy-c213.sh scripts/push-read-replica-policy-c213.sh
git diff --cached --stat
git diff --cached -- Makefile
