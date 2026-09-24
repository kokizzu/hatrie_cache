#!/usr/bin/env bash
set -euo pipefail

GOTOOLCHAIN=auto go test -race ./hat/hatSql -run 'TestSQLColumnarBlockStream'
