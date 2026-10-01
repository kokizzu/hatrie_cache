#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatJournal/parallel_replay.go \
  hat/hatJournal/parallel_replay_test.go \
  hat/hatJournal/parallel_replay_benchmark_test.go
