#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Add ASF copyright line below SPDX header on inherited files.

For files that have an SPDX-License-Identifier header but lack an ASF
copyright attribution, insert a Copyright line below the SPDX line.

Skips files in directories that were created from scratch in this fork
(no upstream apache/flink-statefun heritage). Idempotent: running twice
does not double-insert.
"""

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Directories created from scratch in this fork — no ASF heritage.
EXCLUDE_PATHS = {
    'statefun-docker',
    'statefun-e2e-tests/statefun-e2e-k8s-native',
    'dev',
    'docs/superpowers',
    '.github',
    '.git',
    '.idea',
    '.claude',
    '.cache',
    'target',
    'node_modules',
    'venv',
    '_build',
    'generated',
    'fastutil',
    'bin',
}

# Specific files to skip even if in inherited directories.
EXCLUDE_FILES = {
    'tools/spdx_migrate.py',
    'tools/add_asf_copyright.py',
    'NOTICE',
    'LICENSE',
    'CHANGELOG.md',
    'KZMLABS-GUIDE.md',
    'RELEASE-GUIDE.md',
    'CITATION.cff',
    '.gitignore',
}

COPYRIGHT_TEXT = "Copyright 2014 The Apache Software Foundation"


def should_skip(rel_path):
    """Return True if file should be skipped (our fork's original work)."""
    parts = Path(rel_path).parts
    # Top-level dir match
    if parts and parts[0] in EXCLUDE_PATHS:
        return True
    # Two-level dir match (e.g. docs/superpowers)
    if len(parts) >= 2 and f"{parts[0]}/{parts[1]}" in EXCLUDE_PATHS:
        return True
    if rel_path in EXCLUDE_FILES:
        return True
    return False


# Patterns to detect SPDX line and insert copyright below.
# Each tuple: (regex matching SPDX line at top, replacement format string with {sp} for SPDX line and {cp} for copyright line)
PATTERNS = [
    # // SPDX...
    (re.compile(r'\A(?P<sp>// SPDX-License-Identifier: Apache-2\.0\n)'),
     "{sp}// " + COPYRIGHT_TEXT + "\n"),
    # <!-- SPDX... -->  optionally preceded by <?xml ?> and/or YAML frontmatter
    (re.compile(r'\A(?P<prefix>(?:<\?xml[^\n]*\n)?(?:---\n(?:.*\n)*?---\n)?)'
                r'(?P<sp><!-- SPDX-License-Identifier: Apache-2\.0 -->\n)'),
     "{prefix}{sp}<!-- " + COPYRIGHT_TEXT + " -->\n"),
    # {{/* SPDX... */}}
    (re.compile(r'\A(?P<sp>\{\{/\* SPDX-License-Identifier: Apache-2\.0 \*/\}\}\n)'),
     "{sp}{{{{/* " + COPYRIGHT_TEXT + " */}}}}\n"),
    # # SPDX...   (optionally preceded by shebang, optionally followed by blank)
    (re.compile(r'\A(?P<shebang>#![^\n]*\n)?(?P<sp># SPDX-License-Identifier: Apache-2\.0\n)'),
     "{shebang}{sp}# " + COPYRIGHT_TEXT + "\n"),
]

ALREADY_HAS_COPYRIGHT = re.compile(
    r'(?:Copyright|copyright)[^\n]*Apache Software Foundation', re.IGNORECASE
)


def already_has_copyright(text):
    """Check if the file already has an ASF copyright in its first ~2KB."""
    return bool(ALREADY_HAS_COPYRIGHT.search(text[:2048]))


def process_file(path):
    rel = str(path.relative_to(REPO_ROOT)).replace('\\', '/')
    if should_skip(rel):
        return False
    try:
        raw = path.read_bytes()
    except (PermissionError, OSError):
        return False

    bom = b''
    if raw.startswith(b'\xef\xbb\xbf'):
        bom = b'\xef\xbb\xbf'
        raw = raw[3:]

    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError:
        return False

    crlf = b'\r\n' in raw
    if crlf:
        text = text.replace('\r\n', '\n')

    # Idempotent guard: skip if file already has ASF copyright near top
    if already_has_copyright(text):
        return False

    new_text = None
    for pattern, replacement in PATTERNS:
        m = pattern.match(text)
        if not m:
            continue
        groups = m.groupdict()
        groups = {k: (v or '') for k, v in groups.items()}
        prefix_text = replacement.format(**groups)
        new_text = prefix_text + text[m.end():]
        break

    if new_text is None or new_text == text:
        return False

    if crlf:
        new_text = new_text.replace('\n', '\r\n')
    path.write_bytes(bom + new_text.encode('utf-8'))
    return True


def main():
    changed = 0
    skipped = 0
    by_ext = {}
    skipped_paths = []

    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        rel_dir = Path(dirpath).relative_to(REPO_ROOT)
        # Prune excluded dirs
        dirnames[:] = [d for d in dirnames if not should_skip(
            str(rel_dir / d).replace('\\', '/') if str(rel_dir) != '.' else d)]
        for fn in filenames:
            path = Path(dirpath) / fn
            if process_file(path):
                changed += 1
                ext = path.suffix.lower() or path.name
                by_ext[ext] = by_ext.get(ext, 0) + 1
            else:
                skipped += 1

    print(f"Files modified: {changed}")
    print(f"Files skipped:  {skipped}")
    print()
    print("By extension:")
    for ext, n in sorted(by_ext.items(), key=lambda kv: -kv[1]):
        print(f"  {ext}: {n}")


if __name__ == '__main__':
    main()
