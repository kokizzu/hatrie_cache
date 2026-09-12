#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/key_watchers.go hat/hatCache/key_watcher_filters_test.go
