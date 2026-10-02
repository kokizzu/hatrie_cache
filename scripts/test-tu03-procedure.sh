#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatProcedure -run 'TestRegistry' -count=1
