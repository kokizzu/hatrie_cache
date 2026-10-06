#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run '^TestTU21VersionedMigration' -count=1
