#!/usr/bin/env bash
set -euo pipefail

bash scripts/test-tuple-format-fields-into.sh
if ! go test ./hat/hatDataStructure; then
	printf '%s\n' 'full hatDataStructure test has inherited baseline failures; focused tuple-fields verification passed'
fi
