#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run '^TestSnapshotRestoreWorker(Configuration|Count)$' -count=1
