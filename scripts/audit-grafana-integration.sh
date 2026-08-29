#!/bin/sh
set -eu

grep -n -E 'HandleFunc\("/api/(sql|catalog)|func \(handler \*MonitoringHandler\) handleSQL' hat/hatCache/monitoring.go
sed -n '1010,1170p' hat/hatCache/monitoring.go
grep -n -E 'type SQLCatalog|type SQLQueryRequest|type SQLQueryResult' hat/hatCache/*.go
