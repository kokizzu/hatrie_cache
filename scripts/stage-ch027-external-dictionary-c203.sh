#!/usr/bin/env bash
set -euo pipefail

git add \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_BACKLOG.md \
  BENCHMARK.md \
  CH027_EXTERNAL_DICTIONARY_CACHE.md \
  hat/hatDictionary/dictionary.go \
  hat/hatDictionary/ch027_external_dictionary_test.go \
  hat/hatDictionary/ch027_external_dictionary_benchmark_test.go \
  scripts/benchmark-ch027-external-dictionary-c203.sh \
  scripts/commit-ch027-external-dictionary-c203.sh \
  scripts/format-ch027-external-dictionary-c203.sh \
  scripts/inspect-ch027-external-dictionary-c203.sh \
  scripts/inspect-staged-ch027-external-dictionary-c203.sh \
  scripts/push-ch027-external-dictionary-c203.sh \
  scripts/stage-ch027-external-dictionary-c203.sh \
  scripts/test-ch027-external-dictionary-c203.sh \
  scripts/verify-ch027-external-dictionary-c203.sh

staged_makefile=$(mktemp)
trap 'rm -f "$staged_makefile"' EXIT
git show HEAD:Makefile > "$staged_makefile"
if ! grep -q '^test-ch027-external-dictionary-c203:' "$staged_makefile"; then
  printf '\n\n.PHONY: test-ch027-external-dictionary-c203\ntest-ch027-external-dictionary-c203:\n\t@bash scripts/test-ch027-external-dictionary-c203.sh\n\n.PHONY: inspect-ch027-external-dictionary-c203\ninspect-ch027-external-dictionary-c203:\n\t@bash scripts/inspect-ch027-external-dictionary-c203.sh\n\n.PHONY: format-ch027-external-dictionary-c203\nformat-ch027-external-dictionary-c203:\n\t@bash scripts/format-ch027-external-dictionary-c203.sh\n\n.PHONY: benchmark-ch027-external-dictionary-c203\nbenchmark-ch027-external-dictionary-c203:\n\t@bash scripts/benchmark-ch027-external-dictionary-c203.sh\n\n.PHONY: verify-ch027-external-dictionary-c203\nverify-ch027-external-dictionary-c203:\n\t@bash scripts/verify-ch027-external-dictionary-c203.sh\n\n.PHONY: stage-ch027-external-dictionary-c203\nstage-ch027-external-dictionary-c203:\n\t@bash scripts/stage-ch027-external-dictionary-c203.sh\n\n.PHONY: inspect-staged-ch027-external-dictionary-c203\ninspect-staged-ch027-external-dictionary-c203:\n\t@bash scripts/inspect-staged-ch027-external-dictionary-c203.sh\n\n.PHONY: commit-ch027-external-dictionary-c203\ncommit-ch027-external-dictionary-c203:\n\t@bash scripts/commit-ch027-external-dictionary-c203.sh\n\n.PHONY: push-ch027-external-dictionary-c203\npush-ch027-external-dictionary-c203:\n\t@bash scripts/push-ch027-external-dictionary-c203.sh\n' >> "$staged_makefile"
fi
staged_makefile_blob=$(git hash-object -w "$staged_makefile")
git update-index --cacheinfo 100644,"$staged_makefile_blob",Makefile
git diff --cached --check
