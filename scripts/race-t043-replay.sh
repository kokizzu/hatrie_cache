#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'Test(T042|T043)' -count=1
