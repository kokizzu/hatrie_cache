#!/usr/bin/env bash
set -euo pipefail

git add \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_BACKLOG.md \
  BENCHMARK.md \
  CH028_DICTIONARY_VERSION_FALLBACK.md \
  hat/hatDictionary/dictionary.go \
  hat/hatDictionary/ch028_dictionary_version_test.go \
  hat/hatDictionary/ch028_dictionary_version_benchmark_test.go \
  scripts/benchmark-ch028-dictionary-version-c203.sh \
  scripts/commit-ch028-dictionary-version-c203.sh \
  scripts/format-ch028-dictionary-version-c203.sh \
  scripts/inspect-ch028-dictionary-version-c203.sh \
  scripts/inspect-staged-ch028-dictionary-version-c203.sh \
  scripts/push-ch028-dictionary-version-c203.sh \
  scripts/stage-ch028-dictionary-version-c203.sh \
  scripts/test-ch028-dictionary-version-c203.sh \
  scripts/verify-ch028-dictionary-version-c203.sh

staged_makefile=$(mktemp)
trap 'rm -f "$staged_makefile"' EXIT
git show HEAD:Makefile > "$staged_makefile"
if ! grep -q '^test-ch028-dictionary-version-c203:' "$staged_makefile"; then
  printf '\n\n.PHONY: test-ch028-dictionary-version-c203\ntest-ch028-dictionary-version-c203:\n\t@bash scripts/test-ch028-dictionary-version-c203.sh\n\n.PHONY: inspect-ch028-dictionary-version-c203\ninspect-ch028-dictionary-version-c203:\n\t@bash scripts/inspect-ch028-dictionary-version-c203.sh\n\n.PHONY: format-ch028-dictionary-version-c203\nformat-ch028-dictionary-version-c203:\n\t@bash scripts/format-ch028-dictionary-version-c203.sh\n\n.PHONY: benchmark-ch028-dictionary-version-c203\nbenchmark-ch028-dictionary-version-c203:\n\t@bash scripts/benchmark-ch028-dictionary-version-c203.sh\n\n.PHONY: verify-ch028-dictionary-version-c203\nverify-ch028-dictionary-version-c203:\n\t@bash scripts/verify-ch028-dictionary-version-c203.sh\n\n.PHONY: stage-ch028-dictionary-version-c203\nstage-ch028-dictionary-version-c203:\n\t@bash scripts/stage-ch028-dictionary-version-c203.sh\n\n.PHONY: inspect-staged-ch028-dictionary-version-c203\ninspect-staged-ch028-dictionary-version-c203:\n\t@bash scripts/inspect-staged-ch028-dictionary-version-c203.sh\n\n.PHONY: commit-ch028-dictionary-version-c203\ncommit-ch028-dictionary-version-c203:\n\t@bash scripts/commit-ch028-dictionary-version-c203.sh\n\n.PHONY: push-ch028-dictionary-version-c203\npush-ch028-dictionary-version-c203:\n\t@bash scripts/push-ch028-dictionary-version-c203.sh\n' >> "$staged_makefile"
fi
staged_makefile_blob=$(git hash-object -w "$staged_makefile")
git update-index --cacheinfo 100644,"$staged_makefile_blob",Makefile
git diff --cached --check
