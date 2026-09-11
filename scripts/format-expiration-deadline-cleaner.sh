#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/expiration_cleaner.go hat/hatCache/expiration_deadline_cleaner_test.go hat/hatCache/main.go
