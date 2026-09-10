#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatTopology/topology_commit.go hat/hatCache/topology.go hat/hatCache/topology_commit_test.go hat/hatCache/topology_commit_benchmark_test.go
