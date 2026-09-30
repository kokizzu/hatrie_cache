#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/journal.go \
  hat/hatCache/journal_replay_batch.go \
  hat/hatCache/journal_replay_scalar_decode.go \
  hat/hatCache/t043_replay_scalar_decode_test.go
