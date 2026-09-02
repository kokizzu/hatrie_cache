#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^TestCommandJournal(Request|Tail|Compact|Wire)'
