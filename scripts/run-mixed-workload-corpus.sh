#!/bin/sh
set -eu

go run ./cmd/hatrie-sqlbench -rows 512 -iterations 3
