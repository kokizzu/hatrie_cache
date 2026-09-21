#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run '^TestMaterializedSourceUpsertConflict' -count=1
