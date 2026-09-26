#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCH012ProjectionAdvisor(Forecast|RecordsAutomaticWorkload)' -count=1
