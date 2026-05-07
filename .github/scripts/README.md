# Workflow scripts

Helper scripts called from `.github/workflows/*.yml`. Anything beyond ~10 lines
of inline Python or Bash inside `run: |` lives here so the workflow YAML stays
readable and the script can be tested locally without the workflow harness.

## Inventory

| Script | Used by | Purpose |
|---|---|---|
| `coverage_summary.py` | `ci.yml` | Render JaCoCo coverage tables (unit / e2e / union) into the GitHub Actions job summary. |

## Local testing

Each script reads from concrete paths and writes to the file pointed to by
`GITHUB_STEP_SUMMARY`. To dry-run locally:

```bash
GITHUB_STEP_SUMMARY=/tmp/summary.md python3 .github/scripts/coverage_summary.py union \
  statefun-coverage-report/target/site/jacoco-aggregate/jacoco.xml \
  statefun-e2e-coverage-report/target/site/jacoco-e2e-aggregate/jacoco.xml
cat /tmp/summary.md
```
