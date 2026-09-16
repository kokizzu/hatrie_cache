#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCHU11SQLIndexAdvisor' -count=1
