#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatTopology/quorum_membership.go \
  hat/hatTopology/tu208_anonymous_replica_test.go \
  hat/hatTopology/topology.go \
  hat/hatTopology/tu13_membership_journal.go \
  hat/hatReplication/quorum_members.go \
  hat/hatReplication/tu10_write_quorum.go \
  hat/hatReplication/tu208_anonymous_replica_test.go \
  hat/hatReplication/tu208_anonymous_quorum_baseline_benchmark_test.go \
  hat/hatReplication/tu208_anonymous_quorum_benchmark_test.go
