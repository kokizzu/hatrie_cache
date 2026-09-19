#!/usr/bin/env bash
set -euo pipefail

go test -race -run '^TestCHU39WorkloadAdmission' ./hat/hatSql
