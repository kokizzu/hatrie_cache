#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md IDEA_GAP_CATALOG.md TTG42_PEER_FLOW_CONTROL.md \
  hat/hatReplication/tt42_peer_flow_control.go \
  hat/hatReplication/tt42_peer_flow_control_test.go \
  hat/hatReplication/tt42_peer_flow_control_benchmark_test.go \
  scripts/m237-tt-g42-test.sh scripts/m237-tt-g42-format.sh \
  scripts/m237-tt-g42-benchmark.sh scripts/m237-tt-g42-race.sh \
  scripts/m237-tt-g42-vet.sh scripts/m237-tt-g42-package-test.sh \
  scripts/m237-tt-g42-docs.sh scripts/m237-stage.sh scripts/m237-commit.sh scripts/m237-push.sh
git diff --cached --check
git diff --cached --stat
