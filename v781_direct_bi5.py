#!/usr/bin/env python3
"""
V781 Validation-A Dukascopy native M1 BID acquisition.

Transport-only code. It does not contain or alter any V781 trading rule.
It preserves every downloaded BI5 payload byte-for-byte, then deterministically
normalizes native M1 candle records to UTC CSV.

Native M1 format:
  URL .../{SYMBOL}/{YYYY}/{zero_based_month}/{DD}/BID_candles_min_1.bi5
  LZMA-compressed records, 24 bytes each, big-endian >IIIIIf:
  second_of_day, open, close, low, high, volume.

Frozen scale table:
  EURUSD = 100000
  Dukascopy CFD/crypto instruments in this V781 universe = 1000.
"""
from __future__ import annotations
import argparse, concurrent.futures, csv, datetime as dt, gzip, hashlib, json, lzma
import os, pathlib, struct, time, urllib.error, urllib.request, zipfile

RECORD = struct.Struct(">IIIIIf")
BASE = "https://datafeed.dukascopy.com/datafeed"
SCALE = {
    "EURUSD": 100000,
    "USA500IDXUSD": 1000,
    "USATECHIDXUSD": 1000,
    "DEUIDXEUR": 1000,
    "GBRIDXGBP": 1000,
    "LIGHTCMDUSD": 1000,
    "BTCUSD": 1000,
}
RANGES = {
    "EURUSD": (0.30, 3.0),
    "USA500IDXUSD": (300.0, 15000.0),
    "USATECHIDXUSD": (500.0, 60000.0),
    "DEUIDXEUR": (2000.0, 40000.0),
    "GBRIDXGBP": (2000.0, 20000.0),
    "LIGHTCMDUSD": (2.0, 300.0),
    "BTCUSD": (100.0, 250000.0),
}
UA = "V781-reproducible-validation/1.0 (+raw-bi5-preservation)"

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def sha256_file(path: pathlib.Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for ch in iter(lambda:f.read(1024*1024), b""): h.update(ch)
    return h.hexdigest()

def dates_for_year(year: int):
    d=dt.date(year,1,1)
    end=dt.date(year+1,1,1)
    while d<end:
        yield d
        d += dt.timedelta(days=1)

def url_for(symbol: str, d: dt.date) -> str:
    return f"{BASE}/{symbol}/{d.year}/{d.month-1:02d}/{d.day:02d}/BID_candles_min_1.bi5"

def decompress(payload: bytes) -> bytes:
    errs=[]
    for fmt in (lzma.FORMAT_AUTO, lzma.FORMAT_ALONE):
        try:
            return lzma.decompress(payload, format=fmt)
        except Exception as e:
            errs.append(repr(e))
    raise ValueError("LZMA decode failed: "+" | ".join(errs))

def fetch_one(symbol: str, d: dt.date, raw_root: pathlib.Path):
    url=url_for(symbol,d)
    rel=pathlib.Path(symbol)/str(d.year)/f"{d.month-1:02d}"/f"{d.day:02d}"/"BID_candles_min_1.bi5"
    out=raw_root/rel
    for attempt in range(7):
        req=urllib.request.Request(url,headers={"User-Agent":UA,"Accept":"*/*"})
        try:
            with urllib.request.urlopen(req,timeout=45) as resp:
                code=getattr(resp,"status",200)
                body=resp.read()
            if code != 200:
                raise RuntimeError(f"HTTP {code}")
            if not body:
                return {"date":d.isoformat(),"status":"empty","url":url,"rel":str(rel)}
            decoded=decompress(body)
            if len(decoded)%RECORD.size:
                raise ValueError(f"decoded length {len(decoded)} not divisible by {RECORD.size}")
            out.parent.mkdir(parents=True,exist_ok=True)
            out.write_bytes(body)
            rows=[]
            midnight=dt.datetime(d.year,d.month,d.day,tzinfo=dt.timezone.utc)
            for off in range(0,len(decoded),RECORD.size):
                sec,o,c,lo,hi,vol=RECORD.unpack_from(decoded,off)
                if sec>86399:
                    raise ValueError(f"second_of_day out of range: {sec}")
                rows.append((midnight+dt.timedelta(seconds=sec),o,hi,lo,c,float(vol)))
            return {
                "date":d.isoformat(),"status":"ok","url":url,"rel":str(rel),
                "bytes":len(body),"records":len(rows),"raw_sha256":sha256_bytes(body),
                "rows":rows,
            }
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return {"date":d.isoformat(),"status":"404","url":url,"rel":str(rel)}
            if e.code in (429,500,502,503,504):
                time.sleep(min(20,1.5*(attempt+1)))
                continue
            raise
        except (urllib.error.URLError, TimeoutError):
            if attempt==6: raise
            time.sleep(min(20,1.5*(attempt+1)))
    raise RuntimeError(f"exhausted retries for {url}")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--market",required=True)
    ap.add_argument("--symbol",required=True)
    ap.add_argument("--year",type=int,required=True)
    ap.add_argument("--out",required=True)
    ap.add_argument("--workers",type=int,default=2)
    a=ap.parse_args()
    market=a.market.upper()
    symbol=a.symbol.upper()
    if symbol not in SCALE: raise SystemExit(f"unfrozen symbol {symbol}")
    root=pathlib.Path(a.out)
    root.mkdir(parents=True,exist_ok=True)
    raw_root=root/"raw"
    ledger=[]
    rows=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs={ex.submit(fetch_one,symbol,d,raw_root):d for d in dates_for_year(a.year)}
        for i,f in enumerate(concurrent.futures.as_completed(futs),1):
            item=f.result()
            got=item.pop("rows",None)
            if got: rows.extend(got)
            ledger.append(item)
            if i%50==0: print(f"{market} {a.year}: {i}/{len(futs)} days handled",flush=True)
    ledger.sort(key=lambda x:x["date"])
    rows.sort(key=lambda x:x[0])
    if not rows: raise SystemExit("no native M1 rows acquired")
    scale=SCALE[symbol]
    lo_bound,hi_bound=RANGES[symbol]
    bad=0; dups=0; nonmono=0; prev=None
    first=None; last=None
    normalized=root/f"{market}_{a.year}_M1_BID.csv.gz"
    with gzip.open(normalized,"wt",newline="",encoding="utf-8",compresslevel=9) as gz:
        w=csv.writer(gz,lineterminator="\n")
        w.writerow(["timestamp_utc","open","high","low","close","volume"])
        for ts,o_i,hi_i,lo_i,c_i,vol in rows:
            o=o_i/scale; hi=hi_i/scale; lo=lo_i/scale; c=c_i/scale
            if min(o,hi,lo,c)<=0 or hi<max(o,c) or lo>min(o,c): bad+=1
            if not (lo_bound <= c <= hi_bound): bad+=1
            iso=ts.isoformat().replace("+00:00","Z")
            if prev is not None:
                if ts==prev: dups+=1
                if ts<prev: nonmono+=1
            prev=ts
            first=first or iso; last=iso
            w.writerow([iso,f"{o:.10f}",f"{hi:.10f}",f"{lo:.10f}",f"{c:.10f}",f"{vol:.8f}"])
    if bad or dups or nonmono:
        raise SystemExit(f"normalization audit failed bad={bad} dup={dups} nonmono={nonmono}")
    raw_zip=root/f"{market}_{a.year}_RAW_BI5.zip"
    raw_files=sorted(raw_root.rglob("*.bi5"))
    with zipfile.ZipFile(raw_zip,"w",compression=zipfile.ZIP_STORED,allowZip64=True) as z:
        for p in raw_files:
            z.write(p,p.relative_to(raw_root).as_posix())
    audit={
        "contract":"V781_VALIDATION_A_RAW_M1_BID_V1",
        "market":market,"dukascopy_symbol":symbol,"year":a.year,
        "source_base":BASE,"price_side":"BID","timeframe":"M1","utc_offset":0,
        "native_record_struct":">IIIIIf","native_record_order":["second","open","close","low","high","volume"],
        "price_scale":scale,
        "price_scale_rule":"EURUSD=100000; frozen V781 Dukascopy CFD/crypto universe=1000",
        "rows":len(rows),"first_timestamp_utc":first,"last_timestamp_utc":last,
        "bad_rows":bad,"duplicate_timestamps":dups,"nonmonotone_timestamps":nonmono,
        "days_ok":sum(x["status"]=="ok" for x in ledger),
        "days_404":sum(x["status"]=="404" for x in ledger),
        "days_empty":sum(x["status"]=="empty" for x in ledger),
        "normalized_sha256":sha256_file(normalized),
        "raw_zip_sha256":sha256_file(raw_zip),
        "raw_payload_count":len(raw_files),
        "missing_minutes_policy":"never fabricated; source omissions/session closures remain absent",
        "ledger":ledger,
    }
    (root/"AUDIT.json").write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    (root/"SHA256SUMS.txt").write_text(
        f"{audit['normalized_sha256']}  {normalized.name}\n{audit['raw_zip_sha256']}  {raw_zip.name}\n",
        encoding="utf-8")
    print(json.dumps({k:audit[k] for k in audit if k!="ledger"},sort_keys=True))

if __name__=="__main__":
    main()
