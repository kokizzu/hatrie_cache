#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatDataStructure \
	-run '^TestUpsertBatch' \
	-count=1
