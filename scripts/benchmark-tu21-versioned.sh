#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^(BenchmarkTU21BaselinePreviewAndFingerprint|BenchmarkTU21VersionedMigration)' -benchmem -count=5
