#!/bin/sh
set -eu
go test -race ./hat/hatCache -run '^TestCH044' -count=1
