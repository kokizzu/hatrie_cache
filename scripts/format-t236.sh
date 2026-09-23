#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatFiber/mailbox.go \
	hat/hatFiber/t236_mailbox_baseline_benchmark_test.go \
	hat/hatFiber/t236_mailbox_benchmark_test.go \
	hat/hatFiber/t236_mailbox_test.go \
	hat/hatFiber/t236_mailbox_waiters_test.go
