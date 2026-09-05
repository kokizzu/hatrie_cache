#!/usr/bin/env bash
set -euo pipefail

git restore --staged -- Makefile

git apply --cached <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -247,0 +248,28 @@
+.PHONY: test-t147
+test-t147:
+	bash ./scripts/test-t147.sh
+
+.PHONY: bench-t147
+bench-t147:
+	bash ./scripts/bench-t147.sh
+
+.PHONY: review-t147
+review-t147:
+	bash ./scripts/review-t147.sh
+
+.PHONY: format-t147
+format-t147:
+	bash ./scripts/format-t147.sh
+
+.PHONY: stage-t147
+stage-t147:
+	bash ./scripts/stage-t147.sh
+
+.PHONY: commit-t147
+commit-t147:
+	bash ./scripts/commit-t147.sh
+
+.PHONY: push-t147
+push-t147:
+	bash ./scripts/push-t147.sh
+
PATCH

git apply --cached <<'PATCH'
diff --git a/Makefile b/Makefile
--- a/Makefile
+++ b/Makefile
@@ -4458,0 +4459,3 @@
+.PHONY: audit-inspiration-state
+audit-inspiration-state:
+	bash ./scripts/audit-inspiration-state.sh
PATCH

git add \
  BENCHMARK.md \
  INSPIRATION.md \
  README.md \
  hat/hatCache/command.go \
  hat/hatCache/structured_error_code_test.go \
  hat/hatCommand/command.go \
  hat/hatCommand/error_code_wire_test.go \
  hat/hatCommand/wire.go \
  internal/gen/hatriecache/v1/cache.pb.go \
  proto/hatriecache/v1/cache.proto \
  scripts/audit-inspiration-state.sh \
  scripts/bench-t147.sh \
  scripts/commit-t147.sh \
  scripts/format-t147.sh \
  scripts/push-t147.sh \
  scripts/review-t147.sh \
  scripts/stage-t147.sh \
  scripts/test-t147.sh

git diff --cached --check
