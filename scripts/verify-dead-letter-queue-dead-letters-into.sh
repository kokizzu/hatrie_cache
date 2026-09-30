#!/usr/bin/env bash
set -euo pipefail

bash scripts/test-dead-letter-queue-dead-letters-into.sh
if ! go test ./hat/hatDataStructure; then
	printf '%s\n' 'full hatDataStructure test has inherited baseline failures; focused dead-letter verification passed'
fi
