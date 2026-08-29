#!/bin/sh
set -eu

go test . -run '^TestHatrieSQLNotebookIsValidAndReproducible$'
