#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatTopology/tu13_membership.go hat/hatTopology/tu13_membership_test.go hat/hatTopology/tu13_membership_benchmark_test.go
