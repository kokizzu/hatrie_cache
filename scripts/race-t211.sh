#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache ./hat/hatJournal -run '^(TestT211|TestSpaceSyncPolicy|TestValidateOptions)' -count=1
