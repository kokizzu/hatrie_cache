#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CH029_DICTIONARY_BACKED_JOIN.md \
	INSPIRATION_BACKLOG.md \
	README.md \
	hat/hatDictionary/ch029_dictionary_join_test.go \
	hat/hatDictionary/sql_lookup.go \
	hat/hatSql/ch029_dictionary_join_benchmark_test.go \
	hat/hatSql/ch029_dictionary_join_test.go \
	hat/hatSql/query.go \
	scripts/benchmark-ch029-dictionary-join-c203.sh \
	scripts/commit-ch029-dictionary-join-c203.sh \
	scripts/format-ch029-dictionary-join-c203.sh \
	scripts/inspect-ch029-dictionary-join-c203.sh \
	scripts/inspect-staged-ch029-dictionary-join-c203.sh \
	scripts/push-ch029-dictionary-join-c203.sh \
	scripts/stage-ch029-dictionary-join-c203.sh \
	scripts/test-ch029-dictionary-join-c203.sh \
	scripts/verify-ch029-dictionary-join-c203.sh

makefile_stage=$(mktemp)
trap 'rm -f "$makefile_stage"' EXIT
{
	printf '%s\n' \
	'.PHONY: verify-ch029-dictionary-join-c203' \
	'verify-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/verify-ch029-dictionary-join-c203.sh' \
	'.PHONY: benchmark-ch029-dictionary-join-c203' \
	'benchmark-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/benchmark-ch029-dictionary-join-c203.sh' \
	'.PHONY: format-ch029-dictionary-join-c203' \
	'format-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/format-ch029-dictionary-join-c203.sh' \
	'.PHONY: test-ch029-dictionary-join-c203' \
	'test-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/test-ch029-dictionary-join-c203.sh' \
	'.PHONY: inspect-ch029-dictionary-join-c203' \
	'inspect-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/inspect-ch029-dictionary-join-c203.sh' \
	'.PHONY: inspect-staged-ch029-dictionary-join-c203' \
	'inspect-staged-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/inspect-staged-ch029-dictionary-join-c203.sh' \
	'.PHONY: stage-ch029-dictionary-join-c203' \
	'stage-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/stage-ch029-dictionary-join-c203.sh' \
	'.PHONY: commit-ch029-dictionary-join-c203' \
	'commit-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/commit-ch029-dictionary-join-c203.sh' \
	'.PHONY: push-ch029-dictionary-join-c203' \
	'push-ch029-dictionary-join-c203:' \
	$'\tbash ./scripts/push-ch029-dictionary-join-c203.sh'
	git show HEAD:Makefile
} > "$makefile_stage"
makefile_blob=$(git hash-object -w "$makefile_stage")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
