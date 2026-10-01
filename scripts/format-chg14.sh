#!/usr/bin/env bash
set -euo pipefail

gofmt -w ./hat/hatTopology/durable_membership.go ./hat/hatTopology/durable_membership_test.go ./hat/hatTopology/durable_membership_benchmark_test.go
