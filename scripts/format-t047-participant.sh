#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/cluster_write_commit_participant.go hat/hatReplication/cluster_write_commit_participant_test.go hat/hatReplication/cluster_write_commit_participant_benchmark_test.go
