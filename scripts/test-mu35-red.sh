#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestM35SQLSourceFrontierTrackerWaitUntil' -count=1
