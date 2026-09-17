#!/bin/sh
set -eu

go test -race ./hat/hatJournal ./hat/hatCache -run 'TestTR008' -count=1
