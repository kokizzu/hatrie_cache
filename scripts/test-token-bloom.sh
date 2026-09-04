#!/bin/sh
set -eu

go test ./hat/hatDataStructure -run 'TestTokenBloomFilter' -count=1
