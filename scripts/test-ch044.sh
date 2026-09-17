#!/bin/sh
set -eu
go test ./hat/hatCache -run '^TestCH044' -count=1
