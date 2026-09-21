#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestC241IncrementalRepositoryDefaultChunkDeduplication$' -count=1
