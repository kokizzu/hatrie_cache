#!/usr/bin/env bash
set -euo pipefail

git restore --staged -- Makefile

git apply --cached <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -4489,0 +4490,32 @@
+
+.PHONY: test-t045
+test-t045:
+	bash ./scripts/test-t045.sh
+
+.PHONY: format-t045
+format-t045:
+	bash ./scripts/format-t045.sh
+
+.PHONY: test-race-t045
+test-race-t045:
+	bash ./scripts/test-race-t045.sh
+
+.PHONY: vet-t045
+vet-t045:
+	bash ./scripts/vet-t045.sh
+
+.PHONY: review-t045
+review-t045:
+	bash ./scripts/review-t045.sh
+
+.PHONY: stage-t045
+stage-t045:
+	bash ./scripts/stage-t045.sh
+
+.PHONY: commit-t045
+commit-t045:
+	bash ./scripts/commit-t045.sh
+
+.PHONY: push-t045
+push-t045:
+	bash ./scripts/push-t045.sh
PATCH

git add \
  hat/hatCache/journal_crash_fault_injection_test.go \
  scripts/commit-t045.sh \
  scripts/format-t045.sh \
  scripts/push-t045.sh \
  scripts/review-t045.sh \
  scripts/stage-t045.sh \
  scripts/test-race-t045.sh \
  scripts/test-t045.sh \
  scripts/vet-t045.sh \
  INSPIRATION.md

git diff --cached --check
