#!/bin/sh
set -eu
go test ./hat/hatCache -run '^TestCHU23' -count=1
