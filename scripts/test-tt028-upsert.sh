#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^TestMaterializedSourceUpsertConflict' -count=1
