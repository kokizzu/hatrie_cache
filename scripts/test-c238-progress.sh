#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC238MutationDependency' -count=1
