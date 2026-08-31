#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestCommandJournalProjectionWatermark' -count=1
