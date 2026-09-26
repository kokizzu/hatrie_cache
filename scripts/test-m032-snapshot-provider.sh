#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM032MultiSourceSnapshotCoordinatorPinsPublishedGenerationForSQL$' -count=1
