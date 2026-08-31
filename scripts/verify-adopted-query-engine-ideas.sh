#!/bin/sh
set -eu

test -f ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n 'SQLProjectionRetentionFrontier|TypedTableAggregateArrangements|ManagedRefreshScheduler|MVCC Typed-Table Versions|Immutable Parts And Background Merge' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n 'ADOPTED_QUERY_ENGINE_IDEAS.md' README.md
