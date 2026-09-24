#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM046TypedJSONSubcolumnTopN' -count=1
