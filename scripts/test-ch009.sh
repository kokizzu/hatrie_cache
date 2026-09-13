#!/usr/bin/env bash
set -eu

go test ./hat/hatCache -run '^TestCH009' -count=1
