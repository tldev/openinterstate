#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
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
    parser.add_argument("--output-file")
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


def format_bytes(size_bytes: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(size_bytes)
    unit = units[0]
    for candidate in units:
        unit = candidate
        if size < 1024.0 or candidate == units[-1]:
            break
        size /= 1024.0
    if unit == "B":
        return f"{int(size)} {unit}"
    return f"{size:.1f} {unit}"


def log_progress(downloaded_bytes: int, total_bytes: int | None, started_at: float) -> None:
    elapsed = max(time.monotonic() - started_at, 0.001)
    rate = downloaded_bytes / elapsed
    if total_bytes and total_bytes > 0:
        percent = downloaded_bytes / total_bytes * 100.0
        print(
            (
                f"downloaded {format_bytes(downloaded_bytes)} / {format_bytes(total_bytes)} "
                f"({percent:.1f}%) at {format_bytes(int(rate))}/s"
            ),
            file=sys.stderr,
        )
        return
    print(
        f"downloaded {format_bytes(downloaded_bytes)} at {format_bytes(int(rate))}/s",
        file=sys.stderr,
    )


def main() -> int:
    args = parse_args()
    request = Request(args.url, headers={"User-Agent": "openinterstate-ci/1"})
    digest = hashlib.sha256()
    size_bytes = 0
    output_path = Path(args.output_file).resolve() if args.output_file else None
    started_at = time.monotonic()
    next_progress_at = started_at + 10.0

    try:
        with urlopen(request) as response:
            final_url = response.geturl()
            modified_at = isoformat_http_date(response.headers.get("Last-Modified"))
            content_length_header = response.headers.get("Content-Length")
            total_bytes = int(content_length_header) if content_length_header and content_length_header.isdigit() else None
            if total_bytes is not None:
                print(
                    f"starting download of {source_filename(final_url)} ({format_bytes(total_bytes)})",
                    file=sys.stderr,
                )
            else:
                print(f"starting download of {source_filename(final_url)}", file=sys.stderr)
            if output_path is not None:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("wb") as output_fh:
                    while True:
                        chunk = response.read(args.chunk_size)
                        if not chunk:
                            break
                        digest.update(chunk)
                        size_bytes += len(chunk)
                        output_fh.write(chunk)
                        if time.monotonic() >= next_progress_at:
                            log_progress(size_bytes, total_bytes, started_at)
                            next_progress_at = time.monotonic() + 10.0
            else:
                while True:
                    chunk = response.read(args.chunk_size)
                    if not chunk:
                        break
                    digest.update(chunk)
                    size_bytes += len(chunk)
                    sys.stdout.buffer.write(chunk)
                    if time.monotonic() >= next_progress_at:
                        log_progress(size_bytes, total_bytes, started_at)
                        next_progress_at = time.monotonic() + 10.0
                sys.stdout.buffer.flush()
    except BrokenPipeError:
        print("downstream consumer closed while streaming source PBF", file=sys.stderr)
        return 1

    log_progress(size_bytes, total_bytes, started_at)

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
