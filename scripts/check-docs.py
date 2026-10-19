#!/usr/bin/env python3
"""Offline repository Markdown link/heading check. No HTTP or runtime/data access."""
import argparse
import re
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit
import unicodedata

ROOT = Path(__file__).resolve().parents[1]


def prose(source, strip_inline=True):
    """Remove code examples, preserving line numbers for diagnostics."""
    fence = None
    for line in source.splitlines():
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker:
            token = marker[1]
            if fence is None:
                fence = token
            elif token[0] == fence[0] and len(token) >= len(fence):
                fence = None
            yield ""
        elif fence:
            yield ""
        else:
            yield re.sub(r"(`+).*?\1", "", line) if strip_inline else re.sub(r"`+", "", line)


def headings(source):
    result = set(); used = {}
    for line in prose(source, strip_inline=False):
        match = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        if not match:
            continue
        title = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", match[1]).lower()
        slug = "".join(c for c in title if unicodedata.category(c)[0] in "LMN" or c in " _-").replace(" ", "-")
        number = used.get(slug, 0); used[slug] = number + 1
        result.add(slug + ("-" + str(number) if number else ""))
    result.update(re.findall(r'<a\s+(?:id|name)=["\']([^"\']+)["\']', source))
    return result


def destinations(source):
    lines = list(prose(source)); definitions = {}
    for number, line in enumerate(lines, 1):
        match = re.match(r"^\s*\[([^\]]+)\]:\s*(<[^>]+>|\S+)", line)
        if match:
            definitions[match[1].casefold()] = match[2].strip("<>")
            yield number, definitions[match[1].casefold()]
    for number, line in enumerate(lines, 1):
        if re.match(r"^\s*\[[^\]]+\]:", line):
            continue
        for match in re.finditer(r"!?\[[^\]\n]*\]\(\s*(<[^>]+>|[^\s)]+)(?:\s+['\"][^)]*)?\s*\)", line):
            yield number, match[1].strip("<>")
        for match in re.finditer(r"\[([^\]\n]+)\]\[([^\]\n]*)\]", line):
            key = (match[2] or match[1]).casefold()
            yield number, definitions.get(key, "missing-reference:" + key)


def check(paths, root, repository_files=None):
    root = root.resolve(); problems = []; count = 0; cache = {}
    for source_path in sorted(paths):
        if source_path.is_symlink() or not source_path.resolve().is_relative_to(root):
            problems.append((source_path, 1, str(source_path), "unsafe source path")); continue
        source = source_path.read_text(encoding="utf-8")
        for number, destination in destinations(source):
            count += 1
            if destination.startswith("missing-reference:"):
                problems.append((source_path, number, destination, "undefined reference")); continue
            uri = urlsplit(unquote(destination))
            if uri.scheme or uri.netloc:
                continue  # External URLs are intentionally not fetched.
            target = (root / uri.path.lstrip("/") if uri.path.startswith("/") else source_path.parent / uri.path) if uri.path else source_path
            target = target.resolve()
            if not target.is_relative_to(root) or not target.exists():
                problems.append((source_path, number, destination, "missing/outside repository")); continue
            if repository_files is not None and not (target in repository_files or target.is_dir()
                    and any(p.is_relative_to(target) for p in repository_files)):
                problems.append((source_path, number, destination, "ignored/local-only target")); continue
            if uri.fragment and target.suffix.lower() == ".md":
                if target not in cache:
                    cache[target] = headings(target.read_text(encoding="utf-8"))
                if uri.fragment.removeprefix("user-content-") not in cache[target]:
                    problems.append((source_path, number, destination, "missing heading anchor"))
    return count, problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    result = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
                            cwd=ROOT, capture_output=True, check=True)
    files = {ROOT / p.decode() for p in result.stdout.split(b"\0") if p}
    paths = {p for p in files if p.suffix.lower() == ".md"}
    count, problems = check(paths, ROOT, {p.resolve() for p in files})
    for path, line, destination, reason in problems:
        print(f"{path.relative_to(ROOT)}:{line}: {reason}: {destination}")
    print(f"Documentation links: {len(paths)} files, {count} links, {len(problems)} problems; offline only.")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
