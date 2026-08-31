#!/bin/sh
set -eu

git diff --check -- Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md scripts/verify-adopted-query-engine-ideas.sh scripts/inspect-adopted-query-engine-ideas.sh scripts/deliver-adopted-query-engine-ideas.sh
git diff --stat -- Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md scripts/verify-adopted-query-engine-ideas.sh scripts/inspect-adopted-query-engine-ideas.sh scripts/deliver-adopted-query-engine-ideas.sh
git status --short -- Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md scripts/verify-adopted-query-engine-ideas.sh scripts/inspect-adopted-query-engine-ideas.sh scripts/deliver-adopted-query-engine-ideas.sh
