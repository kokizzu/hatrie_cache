#!/bin/sh
set -eu
test -s TR020_VERSIONED_TUPLE.md
rg -n '^# TR-20 Versioned Tuple Boundaries$' TR020_VERSIONED_TUPLE.md
rg -n '^## TR-20: Versioned tuple boundaries$' BENCHMARK.md
rg -n 'TR020_VERSIONED_TUPLE.md|tr-20-versioned-tuple-boundaries' README.md
rg -n '^\| TR-20 \|.*\[x\].*TR020_VERSIONED_TUPLE.md' INSPIRATION_BACKLOG.md
rg -n '^\| TT-026 \|.*VersionedTuple' ENGINE_IDEAS.md
