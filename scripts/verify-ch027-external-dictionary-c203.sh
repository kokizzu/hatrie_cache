#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDictionary -count=1
go test -race ./hat/hatDictionary -count=1
go vet ./hat/hatDictionary
git diff --check -- CH027_EXTERNAL_DICTIONARY_CACHE.md README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_BACKLOG.md BENCHMARK.md Makefile hat/hatDictionary scripts/benchmark-ch027-external-dictionary-c203.sh scripts/commit-ch027-external-dictionary-c203.sh scripts/format-ch027-external-dictionary-c203.sh scripts/inspect-ch027-external-dictionary-c203.sh scripts/inspect-staged-ch027-external-dictionary-c203.sh scripts/push-ch027-external-dictionary-c203.sh scripts/stage-ch027-external-dictionary-c203.sh scripts/test-ch027-external-dictionary-c203.sh scripts/verify-ch027-external-dictionary-c203.sh
