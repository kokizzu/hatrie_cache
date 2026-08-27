#!/bin/sh
set -eu

go test ./hat/hatStorage -run '^(TestSQLAdapterRegistry|TestPersistentStoresImplementSQLAdapterEngineContract)$' -count=1
