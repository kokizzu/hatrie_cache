#!/bin/sh
set -eu

sed -n '13310,13360p' BENCHMARK.md
sed -n '24,40p' README.md
