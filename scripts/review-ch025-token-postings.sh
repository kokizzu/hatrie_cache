#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/format-ch025-token-postings.sh
go test ./hat/hatDataStructure -run '^TestCH025' -count=1
go test ./hat/hatDataStructure -count=1
go test -race ./hat/hatDataStructure -run '^TestCH025' -count=1
go vet ./hat/hatDataStructure
git diff --check
git status --short
