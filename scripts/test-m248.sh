#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMaintainedResultCache' -count=1
