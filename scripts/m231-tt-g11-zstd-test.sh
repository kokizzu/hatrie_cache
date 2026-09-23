#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^(TestTTG11|TestSegmentedCommandJournalZstdCompressionReplaysAndInspects)$' -count=1
