#!/usr/bin/env bash
set -euo pipefail

GOTOOLCHAIN=auto go test ./hat/hatSql -run 'TestSQLColumnarBlockStreamDictionary' -count=1
