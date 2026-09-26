#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM032MultiSourceSnapshotCoordinator' -count=1
