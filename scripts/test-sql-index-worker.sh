#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONIndexRebuildWorker' -count=1
