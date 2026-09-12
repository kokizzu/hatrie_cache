#!/bin/sh
set -eu

exec gofmt -w hat/hatCache/journal.go hat/hatCache/journal_segments.go hat/hatCache/journal_replay_metadata_test.go
