#!/usr/bin/env python3
"""Render JaCoCo coverage tables to GITHUB_STEP_SUMMARY.

Usage:
    coverage_summary.py unit  <jacoco.xml>
    coverage_summary.py e2e   <jacoco.xml>
    coverage_summary.py union <unit-jacoco.xml> <e2e-jacoco.xml>

The "union" mode produces a single SonarQube-style headline number plus a
detail table. A line is "covered" if either flag covers any instruction on
it - the same semantics Codecov uses to merge flags on the dashboard.
"""
from __future__ import annotations

import os
import sys
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, Tuple

LineKey = Tuple[str, str, str]  # (package, sourcefile, line_nr)
LineCounts = Tuple[int, int, int, int]  # (mi, ci, mb, cb)


def read_lines(path: str) -> Dict[LineKey, LineCounts]:
    out: Dict[LineKey, LineCounts] = {}
    for pkg in ET.parse(path).getroot().iter("package"):
        pname = pkg.get("name", "")
        for sf in pkg.findall("sourcefile"):
            sname = sf.get("name", "")
            for ln in sf.findall("line"):
                key = (pname, sname, ln.get("nr", ""))
                out[key] = (
                    int(ln.get("mi", 0)),
                    int(ln.get("ci", 0)),
                    int(ln.get("mb", 0)),
                    int(ln.get("cb", 0)),
                )
    return out


def project_counters(path: str) -> Iterable[ET.Element]:
    """Top-level <counter> elements that JaCoCo emits as project totals."""
    return [c for c in ET.parse(path).getroot() if c.tag == "counter"]


def pct(covered: int, total: int) -> str:
    return f"{100 * covered / total:.1f}%" if total else "n/a"


def write_per_flag_table(out, title: str, source_note: str | None, path: str) -> None:
    out.write(f"## {title}\n\n")
    if source_note:
        out.write(f"{source_note}\n\n")
    out.write("| Metric | Covered | Total | % |\n")
    out.write("|---|---:|---:|---:|\n")
    for c in project_counters(path):
        t = (c.get("type") or "").title()
        m = int(c.get("missed", 0))
        cv = int(c.get("covered", 0))
        out.write(f"| {t} | {cv:,} | {m + cv:,} | **{pct(cv, m + cv)}** |\n")


def write_union_summary(out, unit_path: str, e2e_path: str) -> None:
    unit = read_lines(unit_path)
    e2e = read_lines(e2e_path)

    total_lines = covered_lines = 0
    total_instr = covered_instr = 0
    total_branches = covered_branches = 0
    for key in set(unit) | set(e2e):
        u = unit.get(key, (0, 0, 0, 0))
        e = e2e.get(key, (0, 0, 0, 0))
        total_lines += 1
        if (u[1] + e[1]) > 0:
            covered_lines += 1
        total_instr += max(u[0] + u[1], e[0] + e[1])
        covered_instr += max(u[1], e[1])
        total_branches += max(u[2] + u[3], e[2] + e[3])
        covered_branches += max(u[3], e[3])

    headline = pct(covered_lines, total_lines)

    out.write("## Combined coverage (line-union of unit + e2e)\n\n")
    out.write(f"### **Project total: {headline}**\n\n")
    out.write(
        "Matches the merged total Codecov shows on the project dashboard "
        "([app.codecov.io](https://app.codecov.io/gh/kzmlabs/flink-statefun)). "
        "A line counts as covered if it is hit by either flag.\n\n"
    )
    out.write("| Metric | Covered | Total | % |\n")
    out.write("|---|---:|---:|---:|\n")
    out.write(
        f"| Line | {covered_lines:,} | {total_lines:,} | **{headline}** |\n"
    )
    out.write(
        f"| Instruction | {covered_instr:,} | {total_instr:,} | **{pct(covered_instr, total_instr)}** |\n"
    )
    out.write(
        f"| Branch | {covered_branches:,} | {total_branches:,} | **{pct(covered_branches, total_branches)}** |\n"
    )


def main(argv: list[str]) -> int:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        print("GITHUB_STEP_SUMMARY is not set; nothing to write.", file=sys.stderr)
        return 0

    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2

    mode = argv[1]
    with open(summary_path, "a", encoding="utf-8") as out:
        if mode == "unit":
            write_per_flag_table(
                out,
                title="Coverage report (aggregate)",
                source_note=(
                    "Full HTML report attached as artifact "
                    "`coverage-report-html` (drill-down by module / package / class / line)."
                ),
                path=argv[2],
            )
        elif mode == "e2e":
            write_per_flag_table(
                out,
                title="E2E coverage report (in-process embedded)",
                source_note=(
                    "Source: `statefun-smoke-e2e-embedded` running StateFun in-process "
                    "via `statefun-flink-harness`. Full HTML report attached as artifact "
                    "`coverage-report-e2e-html`."
                ),
                path=argv[2],
            )
        elif mode == "union":
            if len(argv) < 4:
                print("union mode requires both unit and e2e paths", file=sys.stderr)
                return 2
            write_union_summary(out, argv[2], argv[3])
        else:
            print(f"Unknown mode: {mode}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
