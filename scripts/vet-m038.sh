#!/bin/sh
set -eu

cache_dir="/tmp/hatrie-cache-m038-vet-gocache"
trap 'rm -rf "$cache_dir"' EXIT INT TERM
mkdir -p "$cache_dir"
GOCACHE="$cache_dir" go vet ./hat/hatMerkle
