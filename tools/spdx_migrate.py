#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""SPDX license header migration.

Replaces verbose ASF boilerplate with SPDX-License-Identifier one-liner
in every source file across the repo. Preserves attribution via NOTICE
file at repo root.

Idempotent: running twice does not double-replace.
"""

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Directories to skip entirely
EXCLUDE_DIRS = {
    '.git', '.idea', '.claude', '.cache', 'target', 'node_modules', 'venv',
    '_build', 'generated', 'fastutil',  # fastutil has its own copyright
    'bin',  # IDE/Eclipse generated copies of src/
}

# File types we know how to migrate
JAVA_LIKE = ('.java', '.js', '.ts', '.css')
SLASH_LINE_LIKE = ('.scss', '.go', '.mod', '.proto')        # // line comments
HASH_LIKE = ('.sh', '.py', '.yaml', '.yml', '.toml', '.properties')
XML_LIKE = ('.xml', '.html', '.svg')

ASF_MARKER = "Licensed to the Apache Software Foundation"

# Java/JS block comment at top: /* ... */ or /** ... */
# Permits both " * Licensed..." and "* Licensed..." (with or without leading space).
JAVA_HEADER = re.compile(
    r'\A/\*\*?[^\S\n]*\n'
    r'(?:[^\S\n]*\*[^\n]*\n)*?'
    r'[^\S\n]*\* Licensed to the Apache Software Foundation[^\n]*\n'
    r'(?:[^\S\n]*\*[^\n]*\n)*?'
    r'[^\S\n]*\*[^\n]*under the License\.\n'
    r'[^\S\n]*\*/\n?'
)

# XML block comment: <!-- ... ASF ... -->
# Permits leading <?xml ... ?> declaration AND/OR YAML frontmatter (for .md).
XML_HEADER_AT_TOP = re.compile(
    r'\A(?P<decl><\?xml[^\n]*\n)?'
    r'(?P<frontmatter>---\n(?:.*\n)*?---\n)?'
    r'<!--[^\S\n]*\n'
    r'(?:[^\n]*\n)*?'                                           # any lines until ASF
    r'[^\n]*Licensed to the Apache Software Foundation[^\n]*\n'  # ASF anchor
    r'(?:[^\n]*\n)*?'                                           # body
    r'[^\n]*under the License\.\n'                              # closing
    r'-->\n?'                                                   # may or may not have trailing newline
)

# Hugo / Go template comment: {{/* ... ASF ... */}}
HUGO_TEMPLATE_HEADER = re.compile(
    r'\A\{\{/\*\n'
    r'(?:[^\n]*\n)*?'
    r'[^\n]*Licensed to the Apache Software Foundation[^\n]*\n'
    r'(?:[^\n]*\n)*?'
    r'[^\n]*under the License\.\n'
    r'\*/\}\}\n?'
)

# Hash-style header: #-prefixed lines containing ASF marker.
# Permits a blank line between shebang and comment block.
HASH_HEADER = re.compile(
    r'\A(?P<shebang>#![^\n]*\n)?'                               # optional shebang
    r'(?P<gap>\n*)?'                                            # optional blank lines
    r'(?P<comments>(?:#[^\n]*\n)+)'
)

# Slash-slash line comment header (Go, SCSS, sometimes TS):
# // Licensed to the Apache Software Foundation...
# // ...
# // limitations under the License.
SLASH_LINE_HEADER = re.compile(
    r'\A(?P<comments>(?://[^\n]*\n)+)'
)


def migrate_java_like(text):
    """Replace /* ... ASF ... */ block at top with SPDX line."""
    if ASF_MARKER not in text[:4096]:
        return text, False
    m = JAVA_HEADER.match(text)
    if not m:
        return text, False
    return "// SPDX-License-Identifier: Apache-2.0\n" + text[m.end():], True


def migrate_xml(text):
    """Replace <!-- ... ASF ... --> block at top with SPDX comment.

    Also handles Hugo template comments: {{/* ... */}}.
    """
    if ASF_MARKER not in text[:4096]:
        return text, False
    # Try Hugo template comment first (HTML files in Hugo themes)
    m = HUGO_TEMPLATE_HEADER.match(text)
    if m:
        return "{{/* SPDX-License-Identifier: Apache-2.0 */}}\n" + text[m.end():], True
    m = XML_HEADER_AT_TOP.match(text)
    if not m:
        return text, False
    decl = m.group('decl') or ''
    frontmatter = m.group('frontmatter') or ''
    return decl + frontmatter + "<!-- SPDX-License-Identifier: Apache-2.0 -->\n" + text[m.end():], True


def migrate_slash_line(text):
    """Replace consecutive // comments at top containing ASF marker with SPDX line."""
    if ASF_MARKER not in text[:4096]:
        return text, False
    m = SLASH_LINE_HEADER.match(text)
    if not m or ASF_MARKER not in m.group('comments'):
        return text, False
    rest = text[m.end():]
    # Skip blank line after old header if present
    if rest.startswith('\n'):
        rest = rest[1:]
    return "// SPDX-License-Identifier: Apache-2.0\n\n" + rest, True


def migrate_hash_like(text, is_dockerfile=False):
    """Replace #-prefixed ASF header (with optional shebang) with SPDX line."""
    if ASF_MARKER not in text[:4096]:
        return text, False
    m = HASH_HEADER.match(text)
    if not m or ASF_MARKER not in m.group('comments'):
        return text, False
    shebang = m.group('shebang') or ''
    rest = text[m.end():]
    # Skip blank line after old header if present
    if rest.startswith('\n'):
        rest = rest[1:]
    return shebang + "# SPDX-License-Identifier: Apache-2.0\n\n" + rest, True


def migrate_markdown(text):
    """Markdown files have HTML-style comments — same as XML."""
    return migrate_xml(text)


def process_file(path):
    """Read, migrate, write back if changed. Return True if changed."""
    try:
        raw = path.read_bytes()
    except (PermissionError, OSError):
        return False

    # Detect BOM
    bom = b''
    if raw.startswith(b'\xef\xbb\xbf'):
        bom = b'\xef\xbb\xbf'
        raw = raw[3:]

    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError:
        return False

    # Detect line ending style; normalize to LF for matching
    crlf = b'\r\n' in raw
    if crlf:
        text = text.replace('\r\n', '\n')

    name = path.name
    suffix = path.suffix.lower()

    if suffix in JAVA_LIKE:
        new_text, changed = migrate_java_like(text)
    elif suffix in XML_LIKE:
        new_text, changed = migrate_xml(text)
    elif suffix == '.md':
        new_text, changed = migrate_markdown(text)
    elif suffix in SLASH_LINE_LIKE:
        new_text, changed = migrate_slash_line(text)
    elif suffix in HASH_LIKE:
        new_text, changed = migrate_hash_like(text)
    elif name == 'Dockerfile' or name.startswith('Dockerfile.'):
        new_text, changed = migrate_hash_like(text, is_dockerfile=True)
    else:
        return False

    if changed:
        # Restore original line ending style
        if crlf:
            new_text = new_text.replace('\n', '\r\n')
        path.write_bytes(bom + new_text.encode('utf-8'))
    return changed


def walk_repo(root):
    """Yield all candidate source files."""
    for dirpath, dirnames, filenames in os.walk(root):
        # Prune excluded dirs in-place
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for fn in filenames:
            yield Path(dirpath) / fn


def main():
    changed_count = 0
    skipped_count = 0
    by_ext = {}

    for path in walk_repo(REPO_ROOT):
        rel = path.relative_to(REPO_ROOT)
        if process_file(path):
            changed_count += 1
            ext = path.suffix.lower() or path.name
            by_ext[ext] = by_ext.get(ext, 0) + 1
        else:
            skipped_count += 1

    print(f"Files migrated:    {changed_count}")
    print(f"Files unchanged:   {skipped_count}")
    print()
    print("By extension:")
    for ext, n in sorted(by_ext.items(), key=lambda kv: -kv[1]):
        print(f"  {ext}: {n}")


if __name__ == '__main__':
    main()
