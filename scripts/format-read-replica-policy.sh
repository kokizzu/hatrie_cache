#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/read_replica_policy.go hat/hatReplication/read_replica_policy_test.go
