#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestLevelDBStoreEncryptionHidesRecordsAndRestores$' -count=1
