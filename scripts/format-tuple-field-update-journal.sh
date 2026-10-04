#!/bin/sh
set -eu

gofmt -w \
	hat/hatDataStructure/tuple_field_update_journal.go \
	hat/hatDataStructure/tuple_field_update_journal_test.go
