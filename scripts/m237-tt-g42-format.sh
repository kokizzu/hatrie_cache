#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/tt42_peer_flow_control.go hat/hatReplication/tt42_peer_flow_control_test.go hat/hatReplication/tt42_peer_flow_control_benchmark_test.go
