#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestM35SQLSourceFrontierTrackerWaitUntil' -count=1
