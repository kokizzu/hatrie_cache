#!/bin/sh
set -eu

go test . -run 'TestTokenBloomFilterRootAPIIsImportable' -count=1
