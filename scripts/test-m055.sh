#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestTypedTable.*ArrangementCheckpoint' -count=1
