#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql ./hat/hatDictionary
go test -race ./hat/hatSql ./hat/hatDictionary
go vet ./hat/hatSql ./hat/hatDictionary
git diff --check
