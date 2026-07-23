#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Opens (or comments on) a tracking issue when the scheduled image CVE scan
# fails. Called from image-scan.yml with GH_TOKEN, REPO and RUN_URL set.
set -euo pipefail

TITLE="Weekly image CVE scan failed"
BODY="The scheduled Trivy scan of the release-branch image found fixable HIGH/CRITICAL CVEs (or the scan job errored).

Run with the full report: ${RUN_URL}

The scan uses the same gate config as the release workflows (\`HIGH,CRITICAL\` + \`ignore-unfixed\` + \`exit-code: 1\`), so the next release is blocked at the Trivy step until this is fixed."

existing=$(gh issue list --repo "$REPO" --state open \
  --search "in:title \"$TITLE\"" --json number --jq '.[0].number // empty')

if [ -n "$existing" ]; then
  gh issue comment "$existing" --repo "$REPO" --body "Still failing: ${RUN_URL}"
else
  gh issue create --repo "$REPO" --title "$TITLE" --body "$BODY" \
    --label security 2>/dev/null \
    || gh issue create --repo "$REPO" --title "$TITLE" --body "$BODY"
fi
