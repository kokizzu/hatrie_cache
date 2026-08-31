#!/bin/sh
set -eu

rg --files hat/hatSql | sort
printf '\n-- events --\n'
sed -n '1,240p' hat/hatSql/events.go
printf '\n-- columnar contracts --\n'
sed -n '1,270p' hat/hatSql/contracts.go
printf '\n-- subscriptions --\n'
sed -n '1,280p' hat/hatSql/subscription.go
printf '\n-- adaptive --\n'
sed -n '1,300p' hat/hatSql/adaptive.go
printf '\n-- index advisor --\n'
sed -n '1,320p' hat/hatSql/index_advisor.go
printf '\n-- columnar string dispatch --\n'
sed -n '7180,8025p' hat/hatSql/query.go
printf '\n-- segment construction --\n'
rg -n -C 8 'StringBloomFilters|RowsPerSegment|ColumnarNumericSegments' hat/hatCache hat/hatSql --glob '*.go'
