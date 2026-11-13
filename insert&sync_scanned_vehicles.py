#!/usr/bin/env python3
"""
lpr_db.py - local scan log + dashboard sync for the Raspberry Pi LPR scanner.

Uses only the Python standard library (Python 3.12 on Ubuntu 24.04), so there is
nothing to pip install.

--- From your computer-vision code -------------------------------------------
    from lpr_db import insert_scan
    insert_scan("7ABC123", "CA", "Lot 10", 0.97)       # returns the new row id,
                                                       # or None if it was a duplicate

--- From the command line ----------------------------------------------------
    python3 lpr_db.py insert --plate 7ABC123 --state CA --location "Lot 10" --confidence 0.97
    python3 lpr_db.py status
    python3 lpr_db.py sync --dry-run                   # print the next batch, send nothing
    python3 lpr_db.py sync                             # push everything unsynced, then exit
    python3 lpr_db.py sync --loop --interval 30        # keep pushing every 30 s

--- Settings (environment variables) -----------------------------------------
    LPR_DB_PATH          path to parking_lpr.db   (default: next to this script)
    LPR_DASHBOARD_URL    endpoint that accepts the POSTed JSON batch
    LPR_DASHBOARD_TOKEN  optional bearer token
    LPR_DEVICE_ID        name of this Pi          (default: hostname)
    LPR_BATCH_SIZE       rows per request         (default: 50)
    LPR_HTTP_TIMEOUT     seconds                  (default: 15)

--- Run the sync as a service (/etc/systemd/system/lpr-sync.service) ---------
    [Unit]
    Description=LPR dashboard sync
    After=network-online.target
    Wants=network-online.target

    [Service]
    EnvironmentFile=/etc/lpr-sync.env      # LPR_DB_PATH=..., LPR_DASHBOARD_URL=..., LPR_DASHBOARD_TOKEN=...
    ExecStart=/usr/bin/python3 /home/ubuntu/lpr/lpr_db.py sync --loop --interval 30
    Restart=always
    RestartSec=10

    [Install]
    WantedBy=multi-user.target

    sudo systemctl daemon-reload && sudo systemctl enable --now lpr-sync
    journalctl -u lpr-sync -f
"""
from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import re
import socket
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

DB_PATH = os.environ.get("LPR_DB_PATH", str(Path(__file__).resolve().with_name("parking_lpr.db")))
DASHBOARD_URL = os.environ.get("LPR_DASHBOARD_URL", "")
DASHBOARD_TOKEN = os.environ.get("LPR_DASHBOARD_TOKEN", "")
DEVICE_ID = os.environ.get("LPR_DEVICE_ID", socket.gethostname())
BATCH_SIZE = int(os.environ.get("LPR_BATCH_SIZE", "50"))
HTTP_TIMEOUT = float(os.environ.get("LPR_HTTP_TIMEOUT", "15"))

TS_FMT = "%Y-%m-%d %H:%M:%S"
log = logging.getLogger("lpr")


# --------------------------------------------------------------------------- #
# Database helpers
# --------------------------------------------------------------------------- #
@contextlib.contextmanager
def connect(db_path: str = DB_PATH):
    """Open the database, commit on success, roll back on error, always close."""
    if not Path(db_path).exists():  # sqlite3 would silently create an empty file
        raise FileNotFoundError(f"database not found: {db_path}")
    conn = sqlite3.connect(db_path, timeout=10)  # waits up to 10 s if another process is writing
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA synchronous = NORMAL")  # safe with WAL, fewer SD-card writes
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def normalize_plate(plate: str) -> str:
    """Uppercase and drop spaces/dashes, e.g. '7abc-123' -> '7ABC123'."""
    return re.sub(r"[^A-Z0-9]", "", plate.upper())


# --------------------------------------------------------------------------- #
# Insert
# --------------------------------------------------------------------------- #
def insert_scan(
    plate: str,
    state: str = "CA",
    location: str = "",
    confidence: float = 0.0,
    when: datetime | None = None,
    dedupe_seconds: int = 60,
    db_path: str = DB_PATH,
) -> int | None:
    """Record one plate read. Returns the new row id, or None if skipped as a duplicate.

    A read is a duplicate if the same plate + state + location was already logged within
    `dedupe_seconds` (a parked car is seen many times per second by the camera).
    Set dedupe_seconds=0 to log every read. `when` defaults to now, in the Pi's local time.
    """
    plate = normalize_plate(plate)
    state = (state or "").strip().upper()
    location = (location or "").strip()
    if not plate:
        raise ValueError("plate is empty")
    if not state:
        raise ValueError("state/province is empty")
    if not location:
        raise ValueError("location is empty")
    if not 0.0 <= confidence <= 1.0:
        raise ValueError(f"confidence must be between 0 and 1, got {confidence}")

    when = (when or datetime.now()).replace(microsecond=0)
    with connect(db_path) as conn:
        if dedupe_seconds > 0:
            cutoff = (when - timedelta(seconds=dedupe_seconds)).strftime(TS_FMT)
            dup = conn.execute(
                """SELECT 1 FROM plate_scans
                    WHERE license_plate = ? AND state_province = ? AND location = ?
                      AND scan_date || ' ' || scan_time >= ?
                    LIMIT 1""",
                (plate, state, location, cutoff),
            ).fetchone()
            if dup:
                return None
        cur = conn.execute(
            """INSERT INTO plate_scans
                   (license_plate, state_province, scan_date, scan_time, location, confidence)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (plate, state, when.strftime("%Y-%m-%d"), when.strftime("%H:%M:%S"),
             location, round(confidence, 4)),
        )
        return cur.lastrowid


# --------------------------------------------------------------------------- #
# Sync
# --------------------------------------------------------------------------- #
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect would turn the POST into a GET and look like a success, so treat it as an error."""
    def redirect_request(self, *args, **kwargs):
        return None


_opener = urllib.request.build_opener(_NoRedirect)


def _check_url(url: str) -> None:
    if not url:
        raise ValueError("LPR_DASHBOARD_URL is not set")
    parts = urlparse(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"invalid dashboard URL: {url}")
    if DASHBOARD_TOKEN and parts.scheme == "http" and parts.hostname not in ("localhost", "127.0.0.1"):
        raise ValueError("refusing to send the bearer token over plain http - use https")


def _build_payload(rows: list[sqlite3.Row]) -> dict:
    # The Pi's row id plus device_id lets the dashboard de-duplicate (upsert on device_id + id),
    # which matters if a batch is received but the Pi dies before marking it synced.
    return {"device_id": DEVICE_ID, "scans": [dict(r) for r in rows]}


def _post(payload: dict) -> None:
    """POST one batch. Returns normally only on a 2xx response; raises otherwise."""
    headers = {"Content-Type": "application/json", "User-Agent": f"lpr-pi/{DEVICE_ID}"}
    if DASHBOARD_TOKEN:
        headers["Authorization"] = f"Bearer {DASHBOARD_TOKEN}"
    req = urllib.request.Request(
        DASHBOARD_URL, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST"
    )
    with _opener.open(req, timeout=HTTP_TIMEOUT) as resp:
        if not 200 <= resp.status < 300:
            raise OSError(f"unexpected HTTP status {resp.status}")


def sync_once(dry_run: bool = False, db_path: str = DB_PATH) -> int:
    """Send all unsynced scans in batches, oldest first. Returns how many rows were synced.

    Rows are marked synced=1 only after the dashboard answered 2xx, so a failure
    (no network, server error) leaves them queued for the next run.
    """
    _check_url(DASHBOARD_URL)
    total = 0
    while True:
        with connect(db_path) as conn:  # connection is closed while we wait on the network
            rows = conn.execute(
                """SELECT id, license_plate, state_province, scan_date, scan_time, location, confidence
                     FROM plate_scans WHERE synced = 0 ORDER BY id LIMIT ?""",
                (BATCH_SIZE,),
            ).fetchall()
        if not rows:
            return total
        payload = _build_payload(rows)
        if dry_run:
            print(json.dumps(payload, indent=2))
            return len(rows)
        _post(payload)
        with connect(db_path) as conn:
            conn.executemany("UPDATE plate_scans SET synced = 1 WHERE id = ?", [(r["id"],) for r in rows])
        total += len(rows)
        log.info("synced batch of %d (ids %d-%d)", len(rows), rows[0]["id"], rows[-1]["id"])


def run_loop(interval: int) -> None:
    """Sync forever; on failure, back off (doubling, max 5 min) and try again."""
    delay = interval
    while True:
        try:
            sync_once()
            delay = wait = interval
        except (OSError, sqlite3.Error) as exc:  # URLError/HTTPError/timeouts are OSErrors
            wait = delay
            delay = min(delay * 2, 300)
            log.warning("sync failed: %s - retrying in %d s", exc, wait)
        time.sleep(wait)


def status(db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        total, unsynced, oldest = conn.execute(
            "SELECT COUNT(*), SUM(synced = 0), MIN(CASE WHEN synced = 0 THEN id END) FROM plate_scans"
        ).fetchone()
    print(f"database : {db_path}")
    print(f"scans    : {total} total, {unsynced or 0} waiting to sync"
          + (f" (oldest unsynced id {oldest})" if oldest else ""))


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="LPR scan log: insert and sync")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("insert", help="log one plate read")
    p.add_argument("--plate", required=True)
    p.add_argument("--state", default="CA")
    p.add_argument("--location", required=True)
    p.add_argument("--confidence", type=float, required=True)
    p.add_argument("--no-dedupe", action="store_true", help="log even if just seen")

    s = sub.add_parser("sync", help="push unsynced scans to the dashboard")
    s.add_argument("--loop", action="store_true", help="keep running")
    s.add_argument("--interval", type=int, default=30, help="seconds between syncs with --loop")
    s.add_argument("--dry-run", action="store_true", help="print the next batch; send nothing")

    sub.add_parser("status", help="show row counts")
    args = ap.parse_args(argv)

    try:
        if args.cmd == "insert":
            row_id = insert_scan(args.plate, args.state, args.location, args.confidence,
                                 dedupe_seconds=0 if args.no_dedupe else 60)
            print(f"inserted id {row_id}" if row_id else "skipped (duplicate within 60 s)")
        elif args.cmd == "status":
            status()
        elif args.cmd == "sync":
            if args.loop and args.dry_run:
                ap.error("--loop and --dry-run can't be combined")
            if args.loop:
                _check_url(DASHBOARD_URL)
                run_loop(args.interval)
            else:
                n = sync_once(dry_run=args.dry_run)
                print(f"{'would sync' if args.dry_run else 'synced'} {n} scan(s)")
    except KeyboardInterrupt:
        return 130
    except (ValueError, FileNotFoundError, OSError, sqlite3.Error) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
