#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestDifferentialIntersect|ExampleDifferentialIntersect$)' -count=1
