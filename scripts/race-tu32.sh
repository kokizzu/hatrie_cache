#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatFiber -run '^(TestTU(30|31|32)|Example(Channel|Local))$' -count=1
