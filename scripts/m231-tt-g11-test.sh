#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestTTG11DefaultArchivedJournalCompression$' -count=1
