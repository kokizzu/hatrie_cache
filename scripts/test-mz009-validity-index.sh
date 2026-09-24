#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestSQLJSONValidityIndex' -count=1
