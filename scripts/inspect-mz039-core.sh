#!/bin/sh
set -eu

printf '%s\n' '--- pipeline scheduler ---'
sed -n '1,280p' hat/hatPipeline/scheduler.go
printf '%s\n' '--- query options and execution boundaries ---'
sed -n '150,270p' hat/hatSql/query.go
sed -n '560,880p' hat/hatSql/query.go
printf '%s\n' '--- reusable dataflow executor ---'
sed -n '1,180p' hat/hatSql/dataflow_executor.go
