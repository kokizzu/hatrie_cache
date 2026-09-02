#!/bin/sh
set -eu

go test ./hat/hatCache -run 'TestCommandJournalSubmitAsyncCommand' -count=1
