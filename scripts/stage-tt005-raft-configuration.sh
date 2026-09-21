#!/bin/sh
set -eu

git add \
	Makefile \
	ENGINE_IDEAS.md \
	BENCHMARK.md \
	TT005_RAFT_CONFIGURATION_STATE.md \
	hat/hatReplication/tt005_raft_configuration_state.go \
	hat/hatReplication/tt005_raft_configuration_state_test.go \
	hat/hatReplication/tt005_raft_configuration_state_benchmark_test.go \
	scripts/tt005-raft-configuration.sh \
	scripts/stage-tt005-raft-configuration.sh \
	scripts/commit-tt005-raft-configuration.sh \
	scripts/push-tt005-raft-configuration.sh

git status --short
