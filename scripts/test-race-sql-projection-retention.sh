#!/bin/sh
set -eu

go test -race ./hat/hatCache -run 'Test(CommandJournalProjectionWatermark|SQLJournalProjectionRunner)'
