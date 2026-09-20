#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatBackup -run 'TestSnapshotJoin' -count=1
