#!/usr/bin/env python3
"""V781 Validation-A raw Dukascopy Jetta M1 BID acquisition.

TRANSPORT ONLY. This file does not contain or alter V781 trading rules.

For each requested UTC day it:
1. resolves the frozen Dukascopy instrument through the pinned Jetta catalogue;
2. saves the exact HTTP 200 response bytes before decoding;
3. SHA-256 hashes every raw response;
4. deterministically decodes delta-encoded M1 candles;
5. cross-checks every decoded day against the pinned open-source decoder;
6. emits a deterministic yearly CSV.GZ and a complete request/source manifest.

No missing minute is fabricated. 404/empty source days stay absent.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import math
import os
import pathlib
import sys
import time
from typing import Any

import requests

from dukascopy_downloader.client import BASE_URL, DEFAULT_HEADERS, DukascopyClient
from dukascopy_downloader.decode import decode_candles
from dukascopy_downloader.instruments import Instruments
from dukascopy_downloader.timeframes import Chunk

PINNED_DOWNLOADER_COMMIT = "6dc1b07e697e86477721e8d99fcb824fb01d42ab"
EXPECTED_DOWNLOADER_VERSION = "1.1.0"
V783_PREVALIDATION_FREEZE_SHA256 = "40668904396ce27a05c35e1c21cf088a8f74c0e902a752984d82b3e01b4bc29a"
CONTRACT = "V783_VALIDATION_B_JETTA_RAW_M1_BID_V1"


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(p: pathlib.Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def canonical_json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def dates_for_year(year: int):
    d = dt.date(year, 1, 1)
    end = dt.date(year + 1, 1, 1)
    while d < end:
        yield d
        d += dt.timedelta(days=1)


def decimals(mult: float) -> int:
    if mult <= 0:
        raise ValueError("invalid multiplier")
    x = -math.log10(mult)
    if abs(x - round(x)) < 1e-9:
        return max(0, int(round(x)))
    return 10


def decode_native(j: dict, start: dt.datetime, end: dt.datetime):
    times = j.get("times") or []
    n = len(times)
    if n == 0:
        return [], decimals(float(j.get("multiplier", 1e-5)))
    mult = float(j["multiplier"])
    dec = decimals(mult)
    shift = int(j.get("shift") or 60000)
    ts0 = int(j["timestamp"])

    def cumsum(vals):
        out = []
        s = 0
        for v in vals:
            s += int(v)
            out.append(s)
        return out

    td = cumsum(times)
    if len(td) != n:
        raise ValueError("times length mismatch")
    if n > 1 and any(int(x) <= 0 for x in times[1:]):
        raise ValueError("timestamps are not strictly increasing")

    ms = [ts0 + x * shift for x in td]
    lo_ms = int(start.timestamp() * 1000)
    hi_ms = int(end.timestamp() * 1000)
    if ms[0] < lo_ms or ms[-1] >= hi_ms:
        raise ValueError("timestamps outside requested UTC day")

    def series(base_key, delta_key):
        vals = j[delta_key]
        if len(vals) != n:
            raise ValueError(f"{delta_key} length mismatch")
        base = int(round(float(j[base_key]) / mult))
        acc = cumsum(vals)
        return [round((base + x) * mult, dec) for x in acc]

    o = series("open", "opens")
    h = series("high", "highs")
    l = series("low", "lows")
    c = series("close", "closes")
    vols = [float(x) for x in j["volumes"]]
    if len(vols) != n:
        raise ValueError("volumes length mismatch")

    rows = []
    for i in range(n):
        stamp = dt.datetime.fromtimestamp(ms[i] / 1000, tz=dt.timezone.utc)
        rows.append((stamp, o[i], h[i], l[i], c[i], vols[i]))
    return rows, dec


def assert_decoder_parity(j: dict, start: dt.datetime, end: dt.datetime, rows):
    ref = decode_candles(j, start=start, end=end)
    if len(ref) != len(rows):
        raise AssertionError(f"decoder row mismatch ours={len(rows)} ref={len(ref)}")
    for i, row in enumerate(rows):
        r = ref.iloc[i]
        if row[0] != r["time"].to_pydatetime():
            raise AssertionError(f"timestamp parity failure at {i}")
        for k, v in zip(("open", "high", "low", "close", "volume"), row[1:]):
            if not math.isclose(float(v), float(r[k]), rel_tol=0.0, abs_tol=1e-11):
                raise AssertionError(f"{k} parity failure at {i}: {v} vs {r[k]}")


class FixedRate:
    def __init__(self, per_second: float):
        self.interval = 1.0 / max(float(per_second), 0.1)
        self.next_at = 0.0

    def wait(self):
        now = time.monotonic()
        if self.next_at > now:
            time.sleep(self.next_at - now)
        self.next_at = max(now, self.next_at) + self.interval


def fetch_raw(session: requests.Session, url: str, limiter: FixedRate, attempts: int = 8):
    last = None
    for attempt in range(1, attempts + 1):
        limiter.wait()
        try:
            r = session.get(url, timeout=(10, 60))
            status = r.status_code
            waf = r.headers.get("x-amzn-waf-action")
            if status == 200:
                return status, bytes(r.content), dict(r.headers)
            if status == 404:
                return status, bytes(r.content), dict(r.headers)
            if status == 400:
                # Historical 2019-2021 requests should never be "too late".
                raise RuntimeError(f"permanent HTTP 400: {r.text[:300]}")
            last = f"HTTP {status} waf={waf!r} body={r.text[:200]!r}"
            if status in (202, 403, 408, 425, 429) or status >= 500 or waf:
                retry_after = r.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(30.0, 1.5 * (2 ** (attempt - 1)))
                time.sleep(delay)
                continue
            raise RuntimeError(last)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(min(30.0, 1.5 * (2 ** (attempt - 1))))
    raise RuntimeError(f"fetch exhausted retries: {url}: {last}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True)
    ap.add_argument("--symbol", required=True, help="Resolver-friendly symbol such as USA500IDXUSD")
    ap.add_argument("--year", required=True, type=int)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rate", type=float, default=2.0)
    args = ap.parse_args()

    if args.year not in (2019, 2020, 2021):
        raise SystemExit("LOCKBOX REFUSED: this acquisition build is Validation A only (2019-2021)")

    import dukascopy_downloader as duka
    if duka.__version__ != EXPECTED_DOWNLOADER_VERSION:
        raise SystemExit(f"dependency version drift: {duka.__version__} != {EXPECTED_DOWNLOADER_VERSION}")

    root = pathlib.Path(args.out)
    raw_dir = root / "raw_daily_json"
    meta_dir = root / "_meta"
    raw_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    # Resolve symbol through the same pinned catalogue used by the reference decoder.
    resolver_client = DukascopyClient(rate_limit=1.0, max_retries=8)
    inst = Instruments(resolver_client, meta_dir)
    code = inst.resolve(args.symbol)
    details = inst.details(code)
    resolver_client.close()

    (meta_dir / "resolved_instrument.json").write_bytes(canonical_json_bytes({
        "input_symbol": args.symbol,
        "resolved_code": code,
        "details": details,
    }))

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    limiter = FixedRate(args.rate)

    manifest = []
    all_rows = []
    dec = None

    for idx, day in enumerate(dates_for_year(args.year), 1):
        start = dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc)
        end = start + dt.timedelta(days=1)
        path = Chunk("m1", start, end).url_path(code, "BID")
        url = BASE_URL + path

        status, body, headers = fetch_raw(session, url, limiter)
        entry = {
            "date": day.isoformat(),
            "url_path": path,
            "http_status": status,
            "bytes": len(body),
            "sha256": sha256_bytes(body),
            "content_type": headers.get("Content-Type"),
        }

        if status == 200:
            raw_path = raw_dir / f"{day.isoformat()}.json"
            raw_path.write_bytes(body)
            try:
                payload = json.loads(body)
            except Exception as e:
                raise RuntimeError(f"invalid JSON on {day}: {e}") from e
            rows, day_dec = decode_native(payload, start, end)
            assert_decoder_parity(payload, start, end, rows)
            if dec is None:
                dec = day_dec
            elif rows and day_dec != dec:
                raise RuntimeError(f"price-decimal drift {dec} -> {day_dec} on {day}")
            entry["decoded_rows"] = len(rows)
            all_rows.extend(rows)
        elif status == 404:
            # Explicit source absence is preserved in the manifest. No candle is synthesized.
            err_path = raw_dir / f"{day.isoformat()}.http404.bin"
            err_path.write_bytes(body)
            entry["decoded_rows"] = 0
        else:
            raise AssertionError("unreachable status")

        manifest.append(entry)
        if idx % 50 == 0:
            print(f"{args.market} {args.year}: {idx} UTC days source-audited", flush=True)

    session.close()

    all_rows.sort(key=lambda r: r[0])
    if not all_rows:
        raise SystemExit("no M1 rows acquired")
    if dec is None:
        dec = 10

    bad = dup = nonmono = 0
    max_gap_seconds = 0.0
    gaps_gt_3h = 0
    prev = None
    first = last = None
    for row in all_rows:
        ts, o, h, l, c, vol = row
        if min(o, h, l, c) <= 0 or h < max(o, c) or l > min(o, c) or l > h:
            bad += 1
        if prev is not None:
            if ts == prev:
                dup += 1
            if ts < prev:
                nonmono += 1
            gap = (ts - prev).total_seconds()
            max_gap_seconds = max(max_gap_seconds, gap)
            gaps_gt_3h += int(gap > 3 * 3600)
        prev = ts
        first = first or ts
        last = ts

    if bad or dup or nonmono:
        raise SystemExit(f"normalized tape audit failed bad={bad} dup={dup} nonmono={nonmono}")

    out_csv_gz = root / f"{args.market}_{args.year}_M1_BID_JETTA.csv.gz"
    # Deterministic gzip: mtime=0 and blank internal filename.
    with out_csv_gz.open("wb") as raw_out:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_out, compresslevel=9, mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="") as txt:
                w = csv.writer(txt, lineterminator="\n")
                w.writerow(["timestamp_utc", "open", "high", "low", "close", "volume"])
                for ts, o, h, l, c, vol in all_rows:
                    iso = ts.isoformat().replace("+00:00", "Z")
                    w.writerow([
                        iso,
                        f"{o:.{dec}f}",
                        f"{h:.{dec}f}",
                        f"{l:.{dec}f}",
                        f"{c:.{dec}f}",
                        format(vol, ".15g"),
                    ])

    manifest_path = root / "RAW_HTTP_MANIFEST.json"
    manifest_path.write_bytes(canonical_json_bytes(manifest))

    script_sha = sha256_file(pathlib.Path(__file__))
    audit = {
        "contract": CONTRACT,
        "validation_block": "B_2023_2024",
        "v783_prevalidation_freeze_sha256": V783_PREVALIDATION_FREEZE_SHA256,
        "market": args.market,
        "requested_symbol": args.symbol,
        "resolved_dukascopy_code": code,
        "year": args.year,
        "source_base_url": BASE_URL,
        "source_transport": "Dukascopy Jetta JSON API",
        "price_side": "BID",
        "timeframe": "M1",
        "timezone": "UTC",
        "dependency_repo": "tomas-charvat/Dukascopy_downloader",
        "dependency_commit": PINNED_DOWNLOADER_COMMIT,
        "dependency_version": EXPECTED_DOWNLOADER_VERSION,
        "acquisition_script_sha256": script_sha,
        "request_days": len(manifest),
        "http_200_days": sum(x["http_status"] == 200 for x in manifest),
        "http_404_days": sum(x["http_status"] == 404 for x in manifest),
        "source_raw_bytes": sum(x["bytes"] for x in manifest),
        "raw_http_manifest_sha256": sha256_file(manifest_path),
        "normalized_file": out_csv_gz.name,
        "normalized_sha256": sha256_file(out_csv_gz),
        "normalized_rows": len(all_rows),
        "first_timestamp_utc": first.isoformat().replace("+00:00", "Z"),
        "last_timestamp_utc": last.isoformat().replace("+00:00", "Z"),
        "bad_ohlc_rows": bad,
        "duplicate_timestamps": dup,
        "nonmonotone_timestamps": nonmono,
        "max_observed_gap_seconds": max_gap_seconds,
        "gaps_over_3h": gaps_gt_3h,
        "decoder_crosscheck": "every HTTP-200 day matched pinned dukascopy_downloader.decode_candles",
        "missing_minutes_policy": "never fabricated; explicit source omissions and market closures remain absent",
        "lockbox_policy": "this binary refuses years outside 2023-2024; 2025 remains sealed",
    }
    audit_path = root / "AUDIT.json"
    audit_path.write_bytes(canonical_json_bytes(audit))

    sums = [
        (audit["normalized_sha256"], out_csv_gz.name),
        (audit["raw_http_manifest_sha256"], manifest_path.name),
        (sha256_file(audit_path), audit_path.name),
        (sha256_file(meta_dir / "resolved_instrument.json"), "_meta/resolved_instrument.json"),
    ]
    (root / "SHA256SUMS.txt").write_text("".join(f"{h}  {n}\n" for h, n in sums), encoding="utf-8")

    print(json.dumps(audit, sort_keys=True))


if __name__ == "__main__":
    main()
