#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatReplication/replay_digest.go hat/hatReplication/replay_digest_test.go
