#!/bin/sh
set -eu

go test ./hat/hatJournal ./hat/hatCache -run 'TestTR008' -count=1
