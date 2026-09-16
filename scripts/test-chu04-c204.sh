#!/bin/sh
set -eu
go test ./hat/hatCache -run '^TestC204' -count=1
