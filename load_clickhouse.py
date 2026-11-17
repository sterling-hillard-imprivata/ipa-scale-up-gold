#!/usr/bin/env python3
"""
Batch loader for ClickHouse.

Reads INSERT SQL files from sql-output/ and sends them to ClickHouse in
configurable chunks via the HTTP interface (no clickhouse-client binary needed).

Usage:
    python3 load_clickhouse.py --host <host> --password <password>
    python3 load_clickhouse.py --host <host> --password <pw> --batch-inserts 5
    python3 load_clickhouse.py --host <host> --password <pw> --tables hl7_outbound_events
    python3 load_clickhouse.py --host <host> --password <pw> --dry-run

ClickHouse Cloud example (HTTP interface, port 8443):
    python3 load_clickhouse.py \\
        --host abc123.us-east-1.aws.clickhouse.cloud \\
        --port 8443 \\
        --user sterling_loader \\
        --password <password> \\
        --database patient_secure
"""

import argparse
import base64
import csv
import gzip
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def _iter_inserts(filepath: str):
    """Yield one INSERT statement at a time from a SQL file."""
    buf = []
    with open(filepath) as f:
        for line in f:
            if line.startswith("INSERT INTO") and buf:
                yield "".join(buf)
                buf = []
            buf.append(line)
    if buf:
        yield "".join(buf)


def _make_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    # ClickHouse Cloud uses valid certs, but Python 3.12 TLS renegotiation
    # can cause UNEXPECTED_EOF on some network paths — allow legacy negotiation.
    ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0)
    return ctx


def _run_batch(sql: str, host: str, port: int, user: str, password: str,
               database: str, secure: bool):
    scheme = "https" if secure else "http"
    url = f"{scheme}://{host}:{port}/?database={database}"
    data = sql.encode("utf-8")
    creds = base64.b64encode(f"{user}:{password}".encode()).decode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Basic {creds}")
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    req.add_header("Content-Length", str(len(data)))
    ctx = _make_ssl_context() if secure else None
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code}: {body[:500]}")


def _find_targets(input_dir: str, tables_filter: list | None) -> dict:
    """
    Returns {table_name: {'type': 'sql'|'csv', 'files': [path]}}.
    SQL files take precedence over csv.gz when both exist for the same table.
    """
    result = {}
    for fname in sorted(os.listdir(input_dir)):
        fpath = os.path.join(input_dir, fname)
        if fname.endswith(".sql"):
            table = os.path.splitext(fname)[0]
            result[table] = {"type": "sql", "files": [fpath]}
        elif fname.endswith(".csv.gz") and "__" in fname:
            table = fname.split("__")[0]
            if table not in result:
                result[table] = {"type": "csv", "files": []}
            if result[table]["type"] == "csv":
                result[table]["files"].append(fpath)
    if tables_filter:
        result = {t: v for t, v in result.items() if t in tables_filter}
    return result


def _send_csv_chunk(rows: list, header: list, query: str, url: str,
                    creds: str, ctx) -> None:
    """Gzip a batch of CSV rows in memory and POST to ClickHouse."""
    import io
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        w = csv.writer(io.TextIOWrapper(gz, encoding="utf-8", newline=""))
        w.writerow(header)
        w.writerows(rows)
        w.detach()
    data = buf.getvalue()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization",    f"Basic {creds}")
    req.add_header("Content-Encoding", "gzip")
    req.add_header("Content-Type",     "text/plain; charset=utf-8")
    req.add_header("Content-Length",   str(len(data)))
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code}: {body[:500]}")


def _load_csv_file(filepath: str, table_name: str, csv_chunk_rows: int,
                   host: str, port: int, user: str, password: str,
                   database: str, secure: bool, dry_run: bool) -> tuple[int, float]:
    """Load one .csv.gz file in row-batched chunks. Returns (total_rows, elapsed)."""
    t0 = time.monotonic()
    col_list = query = url = creds = ctx = None

    if not dry_run:
        # Read header without loading whole file
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            header = next(csv.reader(f))
        col_list = ", ".join(header)
        query  = f"INSERT INTO {database}.{table_name} ({col_list}) FORMAT CSVWithNames"
        scheme = "https" if secure else "http"
        url    = f"{scheme}://{host}:{port}/?{urllib.parse.urlencode({'query': query})}"
        creds  = base64.b64encode(f"{user}:{password}".encode()).decode()
        ctx    = _make_ssl_context() if secure else None

    total_rows = 0
    batch: list = []

    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            batch.append(row)
            if len(batch) >= csv_chunk_rows:
                if not dry_run:
                    _send_csv_chunk(batch, header, query, url, creds, ctx)
                total_rows += len(batch)
                elapsed = time.monotonic() - t0
                rps = total_rows / elapsed if elapsed > 0 else 0
                print(f"    {total_rows:>10,} rows  {rps:>8,.0f} rows/s", flush=True)
                batch = []
        if batch:
            if not dry_run:
                _send_csv_chunk(batch, header, query, url, creds, ctx)
            total_rows += len(batch)

    return total_rows, time.monotonic() - t0


def load_file(filepath: str, table_name: str, batch_inserts: int,
              host: str, port: int, user: str, password: str,
              database: str, secure: bool, dry_run: bool) -> tuple[int, float]:
    """Load one SQL file in batches. Returns (total_inserts, elapsed_seconds)."""
    t0 = time.monotonic()
    batch = []
    total_inserts = 0
    rows_per_insert = None

    for insert in _iter_inserts(filepath):
        if rows_per_insert is None:
            rows_per_insert = insert.count("\n    (")
        batch.append(insert)
        if len(batch) >= batch_inserts:
            if not dry_run:
                _run_batch("\n".join(batch), host, port, user, password,
                           database, secure)
            total_inserts += len(batch)
            rows_so_far = total_inserts * (rows_per_insert or 1)
            elapsed = time.monotonic() - t0
            rps = rows_so_far / elapsed if elapsed > 0 else 0
            print(f"    {total_inserts:>6} inserts  (~{rows_so_far:>10,} rows)  "
                  f"{rps:>8,.0f} rows/s", flush=True)
            batch = []

    if batch:
        if not dry_run:
            _run_batch("\n".join(batch), host, port, user, password,
                       database, secure)
        total_inserts += len(batch)

    return total_inserts, time.monotonic() - t0


def main():
    parser = argparse.ArgumentParser(
        description="Batch-load IPA SQL files into ClickHouse")
    parser.add_argument("--host",          required=True,
                        help="ClickHouse host")
    parser.add_argument("--port",          type=int, default=8443,
                        help="ClickHouse HTTP port (default: 8443 for HTTPS)")
    parser.add_argument("--user",          default="default",
                        help="ClickHouse user (default: default)")
    parser.add_argument("--password",      default="",
                        help="ClickHouse password")
    parser.add_argument("--database",      default="default",
                        help="ClickHouse database (default: default)")
    parser.add_argument("--secure",        action="store_true", default=True,
                        help="Use TLS (default: on; use --no-secure to disable)")
    parser.add_argument("--no-secure",     dest="secure", action="store_false")
    parser.add_argument("--input-dir",     default="./sql-output",
                        help="Directory containing .sql files (default: ./sql-output)")
    parser.add_argument("--batch-inserts",   type=int, default=10,
                        help="INSERT statements per request for SQL files (default: 10)")
    parser.add_argument("--csv-chunk-rows", type=int, default=50_000,
                        help="Rows per gzip chunk for CSV files (default: 50 000)")
    parser.add_argument("--tables",        nargs="*", default=None,
                        help="Specific tables to load (default: all in input-dir)")
    parser.add_argument("--dry-run",       action="store_true",
                        help="Parse files and report what would be sent, without connecting")
    args = parser.parse_args()

    if args.batch_inserts < 1:
        parser.error("--batch-inserts must be >= 1")

    # Resolve target files
    targets = _find_targets(args.input_dir, args.tables)
    if not targets:
        print(f"ERROR: no .sql or .csv.gz files found in {args.input_dir!r}", file=sys.stderr)
        raise SystemExit(1)

    if args.tables:
        missing = [t for t in args.tables if t not in targets]
        if missing:
            print(f"ERROR: tables not found in {args.input_dir}: {missing}", file=sys.stderr)
            raise SystemExit(1)

    if args.dry_run:
        print("DRY RUN — no data will be sent to ClickHouse\n")

    print(f"Loading {len(targets)} table(s) → {args.host}:{args.port}/{args.database}")
    print(f"Batch size: {args.batch_inserts} INSERTs per request "
          f"(~{args.batch_inserts * 5000:,} rows)\n")

    total_start = time.monotonic()
    summary     = []

    for table_name in sorted(targets):
        info = targets[table_name]
        if info["type"] == "sql":
            filepath = info["files"][0]
            size_mb  = os.path.getsize(filepath) / 1_048_576
            print(f"→ {table_name}  ({size_mb:.0f} MB)")
            try:
                inserts, elapsed = load_file(
                    filepath, table_name, args.batch_inserts,
                    args.host, args.port, args.user, args.password,
                    args.database, args.secure, args.dry_run,
                )
            except RuntimeError as e:
                print(f"\n  ERROR loading {table_name}: {e}", file=sys.stderr)
                raise SystemExit(1)
            summary.append((table_name, inserts, elapsed))
            print(f"  ✓ {inserts:,} inserts in {elapsed:.1f}s\n")
        else:  # csv
            files = info["files"]
            total_size_mb = sum(os.path.getsize(fp) / 1_048_576 for fp in files)
            print(f"→ {table_name}  ({len(files)} csv.gz files, {total_size_mb:.1f} MB total)")
            total_rows    = 0
            total_elapsed = 0.0
            for filepath in files:
                fname      = os.path.basename(filepath)
                event_type = fname[len(table_name) + 2:].removesuffix(".csv.gz")
                size_mb    = os.path.getsize(filepath) / 1_048_576
                print(f"  {event_type}  ({size_mb:.1f} MB)")
                try:
                    rows, elapsed = _load_csv_file(
                        filepath, table_name, args.csv_chunk_rows,
                        args.host, args.port, args.user, args.password,
                        args.database, args.secure, args.dry_run,
                    )
                except RuntimeError as e:
                    print(f"\n  ERROR loading {filepath}: {e}", file=sys.stderr)
                    raise SystemExit(1)
                total_rows    += rows
                total_elapsed += elapsed
                print(f"    ✓ {rows:,} rows in {elapsed:.1f}s")
            summary.append((table_name, total_rows, total_elapsed))
            print(f"  ✓ {total_rows:,} total rows in {total_elapsed:.1f}s\n")

    total_elapsed = time.monotonic() - total_start
    print("─" * 62)
    print(f"  {'table':<36}  {'rows/inserts':>12}  {'time':>6}")
    print("─" * 62)
    for name, count, sec in summary:
        print(f"  {name:<36}  {count:>12,}  {sec:>5.1f}s")
    print("─" * 62)
    print(f"  {'TOTAL':<36}  {sum(c for _,c,_ in summary):>12,}  "
          f"{total_elapsed:>5.1f}s")


if __name__ == "__main__":
    main()
