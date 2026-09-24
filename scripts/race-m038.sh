#!/bin/sh
set -eu

cache_dir="/tmp/hatrie-cache-m038-race-gocache"
trap 'rm -rf "$cache_dir"' EXIT INT TERM
mkdir -p "$cache_dir"
GOCACHE="$cache_dir" go test -race ./hat/hatMerkle -run 'TestMU38' -count=1
