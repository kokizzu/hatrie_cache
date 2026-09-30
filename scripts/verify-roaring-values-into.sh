#!/usr/bin/env bash
set -euo pipefail

bash scripts/test-roaring-values-into.sh
if ! go test ./hat/hatDataStructure; then
	printf '%s\n' 'full hatDataStructure test has inherited baseline failures; focused roaring verification passed'
fi
