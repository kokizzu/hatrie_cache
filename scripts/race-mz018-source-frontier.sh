#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestMZ018SourceFrontier' -count=1
