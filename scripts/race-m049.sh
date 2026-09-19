#!/usr/bin/env bash
set -euo pipefail

go test -race -run '^TestM049' ./hat/hatSql
