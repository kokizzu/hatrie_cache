#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'Test(CommandJournal|T042|T043)' -count=1
