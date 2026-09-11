#!/bin/sh
set -eu

printf '%s\n' 'Idea ledger rows:'
rg -n '^\| (CH|MZ|TT)-[0-9]{3} ' ENGINE_IDEAS.md
printf '%s\n' 'TT-040 implementation symbols:'
rg -n 'SubscribeSpace|readCommandJournalSpaceTailSet' hat/hatCache/journal_subscription.go hat/hatCache/journal_segments.go
rg -n -A9 '^test-tt040-space-changefeed:|^format-tt040-space-changefeed:|^test-race-tt040-space-changefeed:|^benchmark-tt040-space-changefeed:|^vet-tt040-space-changefeed:|^verify-tt040-docs:|^review-tt040-space-changefeed:|^commit-tt040-space-changefeed:|^push-tt040-space-changefeed:' Makefile
printf '%s\n' 'Recent commits:'
git log -8 --oneline
printf '%s\n' 'Worktree:'
git status --short
