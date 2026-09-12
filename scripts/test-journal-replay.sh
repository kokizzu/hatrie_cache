#!/bin/sh
set -eu

exec go test -run '^TestCommandJournalCachesReplayCompactionBoundary$' ./hat/hatCache
