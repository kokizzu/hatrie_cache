#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestC238MutationDependency' -count=1
