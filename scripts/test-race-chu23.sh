#!/bin/sh
set -eu
go test -race ./hat/hatCache -run '^Test(CHU23|CH009AsyncInsert)' -count=1
