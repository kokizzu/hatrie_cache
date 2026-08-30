#!/bin/sh
set -eu

sed -n '13535,13595p' BENCHMARK.md
rg -n -C 2 'metrics-disabled|BorrowedSourceResolver|unobserved' README.md
