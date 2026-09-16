#!/bin/sh
set -eu

go test ./hat/hatCache -run 'TestCHU50' -count=1
