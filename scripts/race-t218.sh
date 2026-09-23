#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT

go test -race -tags=t218 ./hat/hatDataStructure -run 'TestMultiPartTreeIndex' -count=1
