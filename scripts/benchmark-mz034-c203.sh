#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'DifferentialOperators|ExceptDifferentialRows|DifferentialIntersect' -benchmem -count=5
