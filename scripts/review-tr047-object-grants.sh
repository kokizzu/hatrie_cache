#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test -count=1 ./hat/hatAuth ./hat/hatCache
go vet ./hat/hatAuth ./hat/hatCache
