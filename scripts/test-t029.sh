#!/bin/sh
set -eu

go test ./hat/hatCache -run 'TestRunAtomic' -count=1
