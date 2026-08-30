#!/bin/sh
set -eu

rg -n -C 2 'sqlJSONIndexSnapshotForSourceLocked' hat/hatCache/sql_query.go
