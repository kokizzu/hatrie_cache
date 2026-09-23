#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'Test(C238MutationSnapshot|MutationController)'
