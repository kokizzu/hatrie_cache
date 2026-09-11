#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestSQLText.*Prefix' -count=1
