#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatReplication -run '^TestM033DGlobalTimestampSnapshot' -count=1
