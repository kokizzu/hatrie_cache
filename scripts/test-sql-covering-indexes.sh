#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONCoveringIndex' -count=1
