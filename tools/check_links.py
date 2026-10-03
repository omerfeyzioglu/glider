#!/usr/bin/env python3
"""Check relative links in git-tracked Markdown files and their heading anchors."""

from collections import Counter
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"(?<!!)\[[^\]\n]+\]\(([^\s)]+)(?:\s+['\"][^)]*['\"])?\)")
HEADING = re.compile(r"^ {0,3}#{1,6}[ \t]+(.+?)\s*$")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")


def slug(heading):
    """Return the GitHub-style heading fragment used by local documentation."""
    heading = re.sub(r"<[^>]*>", "", heading)
    heading = re.sub(r"!?(?:\[([^]]+)\])\([^)]*\)", r"\1", heading)
    return re.sub(r" +", "-", re.sub(r"[^\w\- ]", "", heading.lower().strip()))


def unfenced_lines(content):
    marker = None
    for line in content.splitlines():
        fence = FENCE.match(line)
        if fence:
            token = fence.group(1)
            if marker is None:
                marker = token
                continue
            if token[0] == marker[0] and len(token) >= len(marker):
                marker = None
                continue
        if marker is None:
            yield line


def extract_links(content):
    """Yield (full link, destination) pairs outside fenced code blocks."""
    for line in unfenced_lines(content):
        for match in LINK.finditer(line):
            yield match.group(0), match.group(1)


def heading_slugs(content):
    counts = Counter()
    anchors = set()
    for line in unfenced_lines(content):
        match = HEADING.match(line)
        if match:
            base = slug(re.sub(r"\s+#+\s*$", "", match.group(1)))
            anchor = base if counts[base] == 0 else f"{base}-{counts[base]}"
            counts[base] += 1
            anchors.add(anchor)
    return anchors


def tracked_markdown():
    names = subprocess.check_output(["git", "ls-files", "-z", "--", "*.md"], cwd=ROOT)
    return [ROOT / name.decode() for name in names.split(b"\0") if name]


def broken_links():
    for source in tracked_markdown():
        content = source.read_text(encoding="utf-8")
        for link, destination in extract_links(content):
            parsed = urlsplit(destination)
            if parsed.scheme or parsed.netloc or destination.startswith("/"):
                continue
            target = source.parent / unquote(parsed.path) if parsed.path else source
            if not target.exists():
                yield source.relative_to(ROOT), link
            elif parsed.fragment and target.suffix.lower() == ".md":
                if unquote(parsed.fragment) not in heading_slugs(target.read_text(encoding="utf-8")):
                    yield source.relative_to(ROOT), link


def main():
    broken = list(broken_links())
    for source, link in broken:
        print(f"{source}: {link}")
    return int(bool(broken))


if __name__ == "__main__":
    sys.exit(main())
