#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'MZ-032 backlog row:'
rg -n '^\| MZ-032 \|' ENGINE_IDEAS.md

printf '%s\n' 'Existing frontier/time/reclocking symbols:'
rg -n -i 'frontier|reclock|event.?time|processing.?time|watermark|as.?of' hat/hatSql hat/hatDataStructure hat/hatPipeline --glob '*.go' --glob '!**/*_test.go' || true

printf '%s\n' 'Existing time-related tests and benchmarks:'
rg -n -i 'frontier|reclock|event.?time|processing.?time|watermark|as.?of' hat/hatSql hat/hatDataStructure hat/hatPipeline --glob '*_test.go' || true

printf '%s\n' 'FrontierRegistry implementation:'
sed -n '1,340p' hat/hatPipeline/frontier_registry.go

printf '%s\n' 'Frontier retention implementation:'
sed -n '1,260p' hat/hatPipeline/frontier_retention.go
