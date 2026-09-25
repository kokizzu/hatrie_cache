#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt024-union-race.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test -race ./hat/hatSql ./hat/hatCache ./hat/hatSchema -run 'TestSQLContainsPhraseORUsesUnionIndex|TestSQLTextPhraseIndexUnionDeduplicatesAndPreservesSourceOrder|TestTT024TextIndexUnionAdapterDeduplicatesAndPreservesSourceOrder' -count=1
