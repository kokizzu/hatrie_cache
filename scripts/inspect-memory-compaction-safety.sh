#!/usr/bin/env sh
set -eu

git status --short
git diff --check
git diff --stat -- api.go BENCHMARK.md README.md Makefile cmd/hatrie-cache/main.go cmd/hatrie-cache/main_test.go hat/hatCache/memory_compaction.go hat/hatCache/memory_compaction_test.go hat/hatCache/luikore__hat-trie/src/ahtable.c hat/hatCache/luikore__hat-trie/src/ahtable.h hat/hatCache/luikore__hat-trie/src/hat-trie.c hat/hatCache/luikore__hat-trie/src/hat-trie.h scripts
