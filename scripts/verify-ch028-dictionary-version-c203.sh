#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDictionary -count=1
go test -race ./hat/hatDictionary -count=1
go vet ./hat/hatDictionary
git diff --check -- CH028_DICTIONARY_VERSION_FALLBACK.md README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_BACKLOG.md BENCHMARK.md Makefile hat/hatDictionary scripts/benchmark-ch028-dictionary-version-c203.sh scripts/commit-ch028-dictionary-version-c203.sh scripts/format-ch028-dictionary-version-c203.sh scripts/inspect-ch028-dictionary-version-c203.sh scripts/inspect-staged-ch028-dictionary-version-c203.sh scripts/push-ch028-dictionary-version-c203.sh scripts/stage-ch028-dictionary-version-c203.sh scripts/test-ch028-dictionary-version-c203.sh scripts/verify-ch028-dictionary-version-c203.sh
