#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run 'TestSnapshotJoin' -count=1
