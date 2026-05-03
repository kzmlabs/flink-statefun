# Submission to sindresorhus/awesome

Ready-to-paste markdown for [sindresorhus/awesome](https://github.com/sindresorhus/awesome) — the master list of awesome lists. Adding `kzmlabs/awesome-statefun` here is the highest-authority backlink available.

## Recommended placement

Section: **Big Data** (alphabetical insertion). The list already includes `awesome-hadoop`, `awesome-spark`, and `awesome-streaming` in this section.

Currently the closest neighbors alphabetically would be:

```
- [Hadoop](…) - …
- [Spark](…) - Apache Spark.
- [StateFun](…) - Apache Stateful Functions and stateful actor frameworks on Apache Flink.   ← insert here
- [Streaming](…) - Stream processing.
```

## The bullet (paste this)

```markdown
- [StateFun](https://github.com/kzmlabs/awesome-statefun#readme) - Apache Stateful Functions and stateful actor frameworks on Apache Flink.
```

## Why this phrasing

- **Title is short and capitalized** — sindresorhus/awesome reviewers reject long or marketing-y titles.
- **Description is a single sentence ending in a period** — repository style requirement.
- **No "awesome" in the description** — explicitly forbidden by their lint.
- **Anchor is `#readme`** — required by `awesome-lint` so the link points at the README, not the repo root.

## Pre-flight checklist (sindresorhus/awesome is strict)

Before opening the PR, verify the **awesome-statefun** repo itself meets the [awesome-lint](https://github.com/sindresorhus/awesome-lint) rules. Run from inside `kzmlabs/awesome-statefun`:

```bash
npx awesome-lint
```

Common issues to fix first:
- [ ] Repo description ≤ 120 characters and starts with capital, ends with period
- [ ] README has a logo at top OR a horizontal-rule separator between intro and TOC
- [ ] Every list entry follows `- [Name](url) - Description.` exactly
- [ ] No broken links, no http (must be https)
- [ ] `CONTRIBUTING.md`, `code-of-conduct.md`, License — all present
- [ ] At least 30 entries in the list (sindresorhus/awesome rejects thin lists)

If the entry count is below 30, add to `awesome-statefun` first — pad with the existing kzmlabs/flink-statefun resources, the upstream apache/flink-statefun docs, the Flink Stateful Functions papers, the SDK READMEs (Java/Python/Go/JS), notable blog posts, and conference talks. Each is a legitimate entry; just one-sentence each.

## How to submit

1. Fork [sindresorhus/awesome](https://github.com/sindresorhus/awesome)
2. Edit `readme.md`, find the **Big Data** subsection, insert the bullet alphabetically
3. Run `npx awesome-lint` on the fork
4. Open a PR with title: `Add StateFun`
5. PR body — sindresorhus prefers terse:

```markdown
Adds awesome-statefun, a curated list for Apache Stateful Functions and
stateful actor frameworks on Apache Flink. Linted clean with awesome-lint.
The list covers SDKs, runtimes, deployment patterns, and learning material.
```

6. Tick every box in the PR template (it auto-rejects unchecked submissions).

## Expected timeline

sindresorhus/awesome PRs sit in queue for 4–12 weeks. Don't bump the PR. Don't @-mention reviewers. The merge eventually happens or you get a one-line "doesn't fit" — both are valid outcomes.
