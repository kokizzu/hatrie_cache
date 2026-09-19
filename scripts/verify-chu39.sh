#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql
go test -race -run '^TestCHU39WorkloadAdmission' ./hat/hatSql
go vet ./hat/hatSql
