#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql ./hat/hatDictionary -run '^TestCH029' -count=1
