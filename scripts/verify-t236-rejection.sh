#!/usr/bin/env bash
set -euo pipefail

rg -n 'T236|Low-Overhead Mailbox|mailbox|rejected' INSPIRATION_ROUND2.md BENCHMARK.md
