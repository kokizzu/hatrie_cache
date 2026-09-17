#!/bin/sh
set -eu

go test ./hat/hatJournal ./hat/hatCache -count=1
