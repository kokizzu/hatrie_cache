#!/bin/sh
set -eu

rg -n -C 3 'SQL|Projection|Documentation|Benchmark' README.md
