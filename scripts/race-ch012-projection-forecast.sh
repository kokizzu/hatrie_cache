#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestCH012ProjectionAdvisor(Forecast|RecordsAutomaticWorkload)' -count=1
