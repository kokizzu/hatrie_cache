#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatProcedure -run 'TestRegistry' -count=1
