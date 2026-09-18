#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Open inspiration candidates (repository inventory):'
printf '%s\n' '--- ENGINE_IDEAS.md ---'
awk '/^[|]/ && ($0 ~ /\| Open/ || $0 ~ /\| open/ || $0 ~ /remains open/ || $0 ~ /remain open/ || $0 ~ /\[ \]/) { print FNR ":" $0 }' ENGINE_IDEAS.md
printf '%s\n' '--- INSPIRATION_BACKLOG.md ---'
awk '/^[|]/ && ($0 ~ /\[ \]/ || $0 ~ /remains open/ || $0 ~ /remain open/ || $0 ~ /deferred/) { print FNR ":" $0 }' INSPIRATION_BACKLOG.md
printf '%s\n' '--- PRODUCT_IDEA_GAPS.md ---'
awk '/^[|]/ && ($0 ~ /\[ \]/ || $0 ~ /Open/ || $0 ~ /open/ || $0 ~ /remain/) { print FNR ":" $0 }' PRODUCT_IDEA_GAPS.md
