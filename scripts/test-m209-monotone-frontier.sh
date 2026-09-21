#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure ./hat/hatReplication ./hat/hatSql -run 'TestM209|TestChangefeedFrontier|TestSQLSourceFrontierTracker' -count=1
