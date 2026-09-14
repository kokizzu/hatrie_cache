#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CH025_TOKEN_POSTINGS_INDEX.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  README.md \
  hat/hatDataStructure/ch025_token_postings_index.go \
  hat/hatDataStructure/ch025_token_postings_index_benchmark_test.go \
  hat/hatDataStructure/ch025_token_postings_index_public_test.go \
  hat/hatDataStructure/ch025_token_postings_index_test.go \
  scripts/benchmark-ch025-token-postings.sh \
  scripts/commit-ch025-token-postings.sh \
  scripts/format-ch025-token-postings.sh \
  scripts/push-ch025-token-postings.sh \
  scripts/race-ch025-token-postings.sh \
  scripts/report-ch025-token-postings-memory.sh \
  scripts/review-ch025-token-postings.sh \
  scripts/test-ch025-token-postings.sh \
  scripts/vet-ch025-token-postings.sh
git commit -m "Add token postings index"
