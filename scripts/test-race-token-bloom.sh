#!/bin/sh
set -eu

go test -race ./hat/hatDataStructure -run 'TestTokenBloomFilter' -count=1
