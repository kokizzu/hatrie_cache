#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'Test(C238MutationSnapshot|MutationController)'
