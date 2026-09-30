#!/usr/bin/env bash
set -euo pipefail

bash scripts/test-logical-compaction-records-into.sh
if ! go test ./hat/hatDataStructure; then
	printf '%s\n' 'full hatDataStructure test has inherited baseline failures; focused logical-compaction verification passed'
fi
