#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatReplication/t202_replica_set_leader_election.go \
	hat/hatReplication/t202_replica_set_leader_election_test.go \
	hat/hatReplication/t202_replica_set_leader_election_benchmark_test.go
