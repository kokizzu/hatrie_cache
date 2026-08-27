#!/bin/sh
set -eu

sh ./scripts/verify-race.sh
sh ./scripts/verify-sql-fuzz.sh
go test -count=1 ./hat/hatBackup ./hat/hatCache -run 'Backup|Restore|Corrupt|Recovery'
