#!/bin/sh
set -eu

go test ./hat/hatCache -run '^(TestExecuteSQLMutation|TestCompileSQL)' -count=1
