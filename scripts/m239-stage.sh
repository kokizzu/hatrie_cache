#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  IDEA_GAP_CATALOG.md \
  CH046_DICTIONARY_NEGATIVE_CACHE.md \
  hat/hatDictionary/dictionary.go \
  hat/hatDictionary/ch046_dictionary_negative_cache_test.go \
  hat/hatDictionary/ch046_dictionary_negative_cache_options_test.go \
  hat/hatDictionary/ch046_dictionary_negative_cache_benchmark_test.go \
  scripts/m239-ch-g46-format.sh \
  scripts/m239-ch-g46-test.sh \
  scripts/m239-ch-g46-benchmark.sh \
  scripts/m239-ch-g46-package-test.sh \
  scripts/m239-ch-g46-race.sh \
  scripts/m239-ch-g46-vet.sh \
  scripts/m239-ch-g46-docs.sh \
  scripts/m239-stage.sh \
  scripts/m239-commit.sh \
  scripts/m239-push.sh
