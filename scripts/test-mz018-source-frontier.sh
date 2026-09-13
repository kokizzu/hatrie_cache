#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestMZ018SourceFrontier' -count=1
