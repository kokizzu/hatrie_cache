#!/usr/bin/env bash
set -euo pipefail

git add -- Makefile README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md MU027_LOGICAL_PUBLICATION.md hat/hatSql/mu027_logical_publication.go hat/hatSql/mu027_logical_publication_test.go hat/hatSql/mu027_logical_publication_benchmark_test.go scripts/format-mu027-logical-publication.sh scripts/test-mu027-logical-publication.sh scripts/test-mu027-package.sh scripts/race-mu027-logical-publication.sh scripts/vet-mu027-logical-publication.sh scripts/benchmark-mu027-logical-publication.sh scripts/verify-mu027-logical-publication.sh scripts/review-mu027-logical-publication.sh scripts/stage-mu027-logical-publication.sh scripts/commit-mu027-logical-publication.sh scripts/push-mu027-logical-publication.sh
