#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatTopology/tt005_raft_configuration.go hat/hatTopology/tt005_raft_configuration_test.go hat/hatTopology/tt005_raft_configuration_benchmark_test.go
