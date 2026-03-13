#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse
from urllib.request import Request, urlopen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream a source PBF to stdout while recording source metadata."
    )
    parser.add_argument("--url", required=True)
    parser.add_argument("--metadata-file", required=True)
    parser.add_argument("--chunk-size", type=int, default=1024 * 1024)
    return parser.parse_args()


def source_filename(url: str) -> str:
    candidate = PurePosixPath(urlparse(url).path).name
    return candidate or "source.osm.pbf"


def isoformat_http_date(value: str | None) -> str:
    if not value:
        return datetime.now(timezone.utc).isoformat()
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return datetime.now(timezone.utc).isoformat()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def main() -> int:
    args = parse_args()
    request = Request(args.url, headers={"User-Agent": "openinterstate-ci/1"})
    digest = hashlib.sha256()
    size_bytes = 0

    try:
        with urlopen(request) as response:
            final_url = response.geturl()
            modified_at = isoformat_http_date(response.headers.get("Last-Modified"))
            while True:
                chunk = response.read(args.chunk_size)
                if not chunk:
                    break
                digest.update(chunk)
                size_bytes += len(chunk)
                sys.stdout.buffer.write(chunk)
        sys.stdout.buffer.flush()
    except BrokenPipeError:
        print("downstream consumer closed while streaming source PBF", file=sys.stderr)
        return 1

    metadata = {
        "path": final_url,
        "filename": source_filename(final_url),
        "size_bytes": size_bytes,
        "modified_at": modified_at,
        "sha256": digest.hexdigest(),
    }
    metadata_path = Path(args.metadata_file)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
