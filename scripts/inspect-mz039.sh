#!/bin/sh
set -eu

printf '%s\n' '--- backlog and existing yield references ---'
rg -n -C 3 'MZ-39|fuel|yield|budget|cooperative' INSPIRATION_BACKLOG.md ENGINE_IDEAS.md hat/hatSql hat/hatPipeline
printf '%s\n' '--- dataflow executor ---'
sed -n '1,240p' hat/hatSql/dataflow_executor.go
printf '%s\n' '--- native dataflow entry points ---'
rg -n -C 4 'executeNative|Execute\(|for .*row|for .*rows' hat/hatSql/*dataflow*.go hat/hatSql/m052*.go
