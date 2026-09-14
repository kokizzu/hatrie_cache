#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestGroupAverageInt64DifferentialRows|ExampleGroupAverageInt64DifferentialRows' -count=1
