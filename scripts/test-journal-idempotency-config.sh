#!/usr/bin/env sh
set -eu

go test ./cmd/hatrie-cache -run '^TestParseConfigJournal'
