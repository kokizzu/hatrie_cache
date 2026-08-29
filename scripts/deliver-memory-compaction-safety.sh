#!/usr/bin/env sh
set -eu

git diff --check
git add -- api.go BENCHMARK.md README.md Makefile cmd/hatrie-cache/main.go cmd/hatrie-cache/main_test.go hat/hatCache/memory_compaction.go hat/hatCache/memory_compaction_test.go hat/hatCache/luikore__hat-trie/src/ahtable.c hat/hatCache/luikore__hat-trie/src/ahtable.h hat/hatCache/luikore__hat-trie/src/hat-trie.c hat/hatCache/luikore__hat-trie/src/hat-trie.h scripts/bench-memory-compaction-safety.sh scripts/deliver-memory-compaction-safety.sh scripts/format-memory-compaction-safety.sh scripts/inspect-allocation-path.sh scripts/inspect-memory-compaction-benchmark.sh scripts/inspect-memory-compaction-docs.sh scripts/inspect-memory-compaction-safety.sh scripts/test-memory-compaction-safety.sh
git diff --cached --check
git commit -m "fix(memory): bound native compaction allocation"
git push
