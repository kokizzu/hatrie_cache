#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatTopology/election.go \
  hat/hatCache/election.go \
  hat/hatTopology/t202_automatic_leader_election_test.go \
  hat/hatTopology/t202_automatic_leader_election_benchmark_test.go
