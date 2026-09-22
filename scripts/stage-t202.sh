#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md \
	BENCHMARK.md \
	T202_REPLICA_SET_LEADER_ELECTION.md \
	hat/hatReplication/t202_replica_set_leader_election.go \
	hat/hatReplication/t202_replica_set_leader_election_test.go \
	hat/hatReplication/t202_replica_set_leader_election_benchmark_test.go \
	scripts/benchmark-t202.sh \
	scripts/test-t202.sh \
	scripts/format-t202.sh \
	scripts/race-t202.sh \
	scripts/vet-t202.sh \
	scripts/test-t202-package.sh \
	scripts/verify-docs-t202.sh \
	scripts/stage-t202.sh \
	scripts/commit-t202.sh \
	scripts/push-t202.sh

git diff --cached --check
git status --short
