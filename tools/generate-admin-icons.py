#!/usr/bin/env python3
"""Generate Harbor's embedded sprite from the pinned official Lucide tarball.

Download once (no package code or lifecycle scripts execute):
  npm pack lucide-static@1.43.0 --ignore-scripts --pack-destination .snow/lucide-static-1.43.0
Then run:
  python tools/generate-admin-icons.py
  python tools/generate-admin-icons.py --check

Python standard library only. No runtime dependency, new asset or server route.
"""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import tarfile
import xml.etree.ElementTree as ET

VERSION = "1.43.0"
INTEGRITY = "sha512-w7vdVFqh4vv7RxWHN4sSeylA0s3GHV5vpyGqZI0q22MINKJyhMNWSMd90RyTUYWBRNYqdZVZzrMmijVxU3Cl9A=="
ROOT = Path(__file__).resolve().parent.parent
HTML = ROOT / "internal/server/ui/admin.html"
LICENSE = ROOT / "tools/LUCIDE-LICENSE.txt"
ARCHIVE = ROOT / f".snow/lucide-static-{VERSION}/lucide-static-{VERSION}.tgz"

# Stable Harbor IDs -> official package/icons/<name>.svg. Existing IDs are retained.
ICONS = {
    "logo": "layers-3",
    "gauge": "gauge",
    "queue": "list-ordered",
    "file": "file-text",
    "key": "key-round",
    "server": "server",
    "plus": "plus",
    "pencil": "pencil",
    "trash": "trash-2",
    "ban": "ban",
    "check": "check",
    "x": "x",
    "home": "house",
    "out": "log-out",
    "refresh": "refresh-cw",
    "zap": "zap",
    "search": "search",
    "branch": "git-branch",
    "menu": "menu",
    "arrow-left": "arrow-left",
    "arrow-right": "arrow-right",
    "arrow-up-right": "arrow-up-right",
    "activity": "activity",
    "circle-check": "circle-check",
    "alert-triangle": "triangle-alert",
    "layers-3": "layers-3",
    "timer": "timer",
    "check-circle-2": "circle-check-big",
    "alert-circle": "circle-alert",
    "arrow-down-to-line": "arrow-down-to-line",
    "arrow-up-from-line": "arrow-up-from-line",
    "database": "database",
    "shield-check": "shield-check",
    "refresh-cw": "refresh-cw",
    "list-ordered": "list-ordered",
    "cpu": "cpu",
    "inbox": "inbox",
    "key-round": "key-round",
    "key-square": "key-square",
    "lock-keyhole": "lock-keyhole",
    "fingerprint": "fingerprint",
    "trash-2": "trash-2",
    "copy": "copy",
    "network": "network",
    "route": "route",
    "chevron-down": "chevron-down",
    "sliders-horizontal": "sliders-horizontal",
    "arrow-up": "arrow-up",
    "arrow-down": "arrow-down",
    "circle-help": "circle-help",
    "loader-circle": "loader-circle",
}

SPRITE = re.compile(r'<svg\b(?=[^>]*\bclass="sprite")[^>]*>[\s\S]*?</svg>')
ATTRS = ("viewBox", "fill", "stroke", "stroke-width", "stroke-linecap", "stroke-linejoin")


def xml_shape(source):
    """Ignore formatting, retain every official geometry/style attribute."""
    def shape(node):
        return (node.tag, sorted(node.attrib.items()), (node.text or "").strip(),
                tuple(shape(child) for child in node))
    return shape(ET.fromstring(source))


def build(archive):
    digest = base64.b64encode(hashlib.sha512(archive.read_bytes()).digest()).decode("ascii")
    if "sha512-" + digest != INTEGRITY:
        raise ValueError("Lucide tarball integrity mismatch; expected pinned official release")
    with tarfile.open(archive, "r:gz") as package:
        def text(name):
            stream = package.extractfile("package/" + name)
            if stream is None:
                raise ValueError("Missing official package file: " + name)
            return stream.read().decode("utf-8")
        metadata = json.loads(text("package.json"))
        if (metadata.get("name"), metadata.get("version")) != ("lucide-static", VERSION):
            raise ValueError("Unexpected package name/version")
        license_text = text("LICENSE")
        symbols = []
        for alias, name in ICONS.items():
            source = text("icons/" + name + ".svg")
            root = ET.fromstring(source)
            if root.attrib.get("viewBox") != "0 0 24 24":
                raise ValueError("Unexpected viewBox: " + name)
            match = re.search(r"<svg\b[^>]*>([\s\S]*?)</svg>", source)
            if not match:
                raise ValueError("Invalid official SVG: " + name)
            # Copy official elements verbatim; only trim indentation between tags.
            body = re.sub(r">\s+<", "><", match.group(1).strip())
            attributes = " ".join(f'{key}="{root.attrib[key]}"' for key in ATTRS)
            symbols.append(f'  <symbol id="i-{alias}" {attributes}>{body}</symbol>')
    # Carry the complete license with the embedded binary too. HTML comments cannot
    # contain '--', so the upstream plain-text separator becomes a blank line here.
    embedded_license = re.sub(r"(?m)^---\s*$", "", license_text).strip()
    if "--" in embedded_license:
        raise ValueError("License cannot safely be embedded in an HTML comment")
    sprite = ('<svg class="sprite" xmlns="http://www.w3.org/2000/svg" '
              'aria-hidden="true" focusable="false">\n'
              f'  <!-- Generated from lucide-static@{VERSION}; see tools/generate-admin-icons.py. -->\n'
              '  <!--\n' + embedded_license + '\n  -->\n' + "\n".join(symbols) + '\n</svg>')
    return sprite, license_text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=ARCHIVE)
    parser.add_argument("--check", action="store_true", help="Verify without modifying files")
    args = parser.parse_args()
    sprite, license_text = build(args.archive)
    html = HTML.read_text(encoding="utf-8")
    matches = list(SPRITE.finditer(html))
    if len(matches) != 1:
        raise ValueError("Expected exactly one bounded sprite in admin.html")
    existing_ids = set(re.findall(r'<symbol\b[^>]*\bid="i-([^"]+)"', matches[0].group()))
    if existing_ids - ICONS.keys():
        raise ValueError("Refusing to remove unmapped existing IDs: " + ", ".join(sorted(existing_ids - ICONS.keys())))
    if args.check:
        if xml_shape(matches[0].group()) != xml_shape(sprite):
            raise ValueError("Embedded sprite differs from official pinned SVG data")
        if LICENSE.read_text(encoding="utf-8") != license_text:
            raise ValueError("Stored license differs from official package/LICENSE")
        if embedded_license_missing(html):
            raise ValueError("Embedded attribution or license missing")
        print(f"OK: {len(ICONS)} official Lucide symbols, integrity, geometry and license")
        return
    HTML.write_text(SPRITE.sub(lambda _: sprite, html, count=1), encoding="utf-8", newline="\n")
    LICENSE.write_text(license_text, encoding="utf-8", newline="\n")
    print(f"Generated {len(ICONS)} official Lucide symbols in {HTML.relative_to(ROOT)}")
    print(f"Copied package/LICENSE to {LICENSE.relative_to(ROOT)}")


def embedded_license_missing(html):
    return any(notice not in html for notice in (
        "Copyright (c) 2026 Lucide Icons and Contributors",
        "Copyright (c) 2013-present Cole Bemis",
        "THE SOFTWARE IS PROVIDED",
        "Permission is hereby granted",
    ))


if __name__ == "__main__":
    main()
