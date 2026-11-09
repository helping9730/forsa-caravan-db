#!/usr/bin/env python3
"""V785 untouched validation raw Dukascopy/Jetta M1 BID acquisition.

DATA TRANSPORT ONLY. No strategy logic.
Preserves exact HTTP response bytes, per-day SHA256, deterministic normalization,
and cross-checks decoding against the pinned dukascopy_downloader implementation.

This build refuses any year except 2025 because V784's 2025 lockbox was frozen
before acquisition.
"""
from __future__ import annotations
import argparse, csv, datetime as dt, gzip, hashlib, io, json, math, pathlib, time
from typing import Any
import requests
from dukascopy_downloader.client import BASE_URL, DEFAULT_HEADERS, DukascopyClient
from dukascopy_downloader.decode import decode_candles
from dukascopy_downloader.instruments import Instruments
from dukascopy_downloader.timeframes import Chunk

PINNED_DOWNLOADER_COMMIT="6dc1b07e697e86477721e8d99fcb824fb01d42ab"
EXPECTED_DOWNLOADER_VERSION="1.1.0"
V787_PRESTRESS_FREEZE_SHA256="5db18e8f2fdc1c3d84e2e183e3da391bf012e87d0670e7418a82e8c38bf0dcad"
CONTRACT="V787_STRESS_2018_2022_JETTA_RAW_M1_BID_V1"

def sha256_bytes(b:bytes)->str: return hashlib.sha256(b).hexdigest()
def sha256_file(p:pathlib.Path)->str:
    h=hashlib.sha256()
    with p.open("rb") as f:
        for block in iter(lambda:f.read(1024*1024),b""): h.update(block)
    return h.hexdigest()
def canonical(obj:Any)->bytes:
    return (json.dumps(obj,sort_keys=True,separators=(",",":"),ensure_ascii=False)+"\n").encode()
def dates_for_year(y:int):
    d=dt.date(y,1,1); e=dt.date(y+1,1,1)
    while d<e:
        yield d; d+=dt.timedelta(days=1)
def decimals(mult:float)->int:
    x=-math.log10(mult)
    return max(0,int(round(x))) if abs(x-round(x))<1e-9 else 10
def decode_native(j,start,end):
    times=j.get("times") or []; n=len(times)
    if n==0: return [],decimals(float(j.get("multiplier",1e-5)))
    mult=float(j["multiplier"]); dec=decimals(mult); shift=int(j.get("shift") or 60000); ts0=int(j["timestamp"])
    def cs(vals):
        out=[]; s=0
        for v in vals: s+=int(v); out.append(s)
        return out
    td=cs(times)
    if n>1 and any(int(x)<=0 for x in times[1:]): raise ValueError("timestamps not strictly increasing")
    ms=[ts0+x*shift for x in td]
    lo=int(start.timestamp()*1000); hi=int(end.timestamp()*1000)
    if ms[0]<lo or ms[-1]>=hi: raise ValueError("timestamp outside requested UTC day")
    def ser(base_key,delta_key):
        vals=j[delta_key]
        if len(vals)!=n: raise ValueError(delta_key+" length mismatch")
        base=int(round(float(j[base_key])/mult)); acc=cs(vals)
        return [round((base+x)*mult,dec) for x in acc]
    o=ser("open","opens"); h=ser("high","highs"); l=ser("low","lows"); c=ser("close","closes")
    vols=[float(x) for x in j["volumes"]]
    rows=[]
    for i in range(n):
        rows.append((dt.datetime.fromtimestamp(ms[i]/1000,tz=dt.timezone.utc),o[i],h[i],l[i],c[i],vols[i]))
    return rows,dec
def parity(j,start,end,rows):
    ref=decode_candles(j,start=start,end=end)
    if len(ref)!=len(rows): raise AssertionError("decoder row mismatch")
    for i,row in enumerate(rows):
        rr=ref.iloc[i]
        if row[0] != rr["time"].to_pydatetime(): raise AssertionError(f"time parity {i}")
        for k,v in zip(("open","high","low","close","volume"),row[1:]):
            if not math.isclose(float(v),float(rr[k]),rel_tol=0,abs_tol=1e-11):
                raise AssertionError(f"{k} parity {i}")
class Rate:
    def __init__(self,r): self.dt=1/max(float(r),0.1); self.next=0.0
    def wait(self):
        now=time.monotonic()
        if self.next>now: time.sleep(self.next-now)
        self.next=max(now,self.next)+self.dt
def fetch(session,url,limiter,attempts=8):
    last=None
    for a in range(1,attempts+1):
        limiter.wait()
        try:
            r=session.get(url,timeout=(10,60)); status=r.status_code
            if status in (200,404): return status,bytes(r.content),dict(r.headers)
            if status==400: raise RuntimeError("permanent HTTP400 "+r.text[:300])
            last=f"HTTP {status} body={r.text[:200]!r}"
            if status in (202,403,408,425,429) or status>=500 or r.headers.get("x-amzn-waf-action"):
                ra=r.headers.get("Retry-After")
                delay=float(ra) if ra and ra.isdigit() else min(30.0,1.5*(2**(a-1)))
                time.sleep(delay); continue
            raise RuntimeError(last)
        except (requests.ConnectionError,requests.Timeout,requests.exceptions.ChunkedEncodingError) as e:
            last=f"{type(e).__name__}: {e}"; time.sleep(min(30.0,1.5*(2**(a-1))))
    raise RuntimeError("fetch exhausted "+str(last))
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--market",required=True); ap.add_argument("--symbol",required=True)
    ap.add_argument("--year",required=True,type=int); ap.add_argument("--out",required=True)
    ap.add_argument("--rate",type=float,default=1.5)
    a=ap.parse_args()
    if a.year not in (2018,2022): raise SystemExit("STRESS REFUSED: this acquisition build only permits 2018 or 2022")
    import dukascopy_downloader as duka
    if duka.__version__!=EXPECTED_DOWNLOADER_VERSION: raise SystemExit("dependency version drift")
    root=pathlib.Path(a.out); rawdir=root/"raw_daily_json"; metadir=root/"_meta"
    rawdir.mkdir(parents=True,exist_ok=True); metadir.mkdir(parents=True,exist_ok=True)
    dc=DukascopyClient(rate_limit=1.0,max_retries=8); inst=Instruments(dc,metadir)
    code=inst.resolve(a.symbol); details=inst.details(code); dc.close()
    (metadir/"resolved_instrument.json").write_bytes(canonical({"input_symbol":a.symbol,"resolved_code":code,"details":details}))
    ses=requests.Session(); ses.headers.update(DEFAULT_HEADERS); limiter=Rate(a.rate)
    manifest=[]; all_rows=[]; dec=None
    for idx,day in enumerate(dates_for_year(a.year),1):
        start=dt.datetime(day.year,day.month,day.day,tzinfo=dt.timezone.utc); end=start+dt.timedelta(days=1)
        path=Chunk("m1",start,end).url_path(code,"BID"); url=BASE_URL+path
        status,body,headers=fetch(ses,url,limiter)
        ent={"date":day.isoformat(),"url_path":path,"http_status":status,"bytes":len(body),"sha256":sha256_bytes(body),"content_type":headers.get("Content-Type")}
        if status==200:
            rp=rawdir/f"{day.isoformat()}.json"; rp.write_bytes(body)
            payload=json.loads(body); rows,d=decode_native(payload,start,end); parity(payload,start,end,rows)
            if dec is None: dec=d
            elif rows and d!=dec: raise RuntimeError("price decimal drift")
            ent["decoded_rows"]=len(rows); all_rows.extend(rows)
        else:
            (rawdir/f"{day.isoformat()}.http404.bin").write_bytes(body); ent["decoded_rows"]=0
        manifest.append(ent)
        if idx%50==0: print(a.market,a.year,idx,"days audited",flush=True)
    ses.close()
    all_rows.sort(key=lambda x:x[0])
    if not all_rows: raise SystemExit("no rows")
    if dec is None: dec=10
    bad=dup=nonmono=0; prev=None; first=None; last=None; maxgap=0; gaps3=0
    for row in all_rows:
        ts,o,h,l,c,v=row
        bad += int(min(o,h,l,c)<=0 or h<max(o,c) or l>min(o,c) or l>h)
        if prev is not None:
            dup += int(ts==prev); nonmono += int(ts<prev)
            gap=(ts-prev).total_seconds(); maxgap=max(maxgap,gap); gaps3+=int(gap>10800)
        prev=ts; first=first or ts; last=ts
    if bad or dup or nonmono: raise SystemExit(f"audit failed bad={bad} dup={dup} nonmono={nonmono}")
    outgz=root/f"{a.market}_{a.year}_M1_BID_JETTA.csv.gz"
    with outgz.open("wb") as raw:
        with gzip.GzipFile(filename="",mode="wb",fileobj=raw,compresslevel=9,mtime=0) as gz:
            with io.TextIOWrapper(gz,encoding="utf-8",newline="") as txt:
                w=csv.writer(txt,lineterminator="\n"); w.writerow(["timestamp_utc","open","high","low","close","volume"])
                for ts,o,h,l,c,v in all_rows:
                    w.writerow([ts.isoformat().replace("+00:00","Z"),f"{o:.{dec}f}",f"{h:.{dec}f}",f"{l:.{dec}f}",f"{c:.{dec}f}",format(v,".15g")])
    mp=root/"RAW_HTTP_MANIFEST.json"; mp.write_bytes(canonical(manifest))
    audit={
      "contract":CONTRACT,"validation_block":"C_2015W_2016_2017","validation_role":("WARMUP_ONLY" if a.year==2015 else "EVALUATION"),"v785_prevalidation_freeze_sha256":V787_PRESTRESS_FREEZE_SHA256,
      "market":a.market,"requested_symbol":a.symbol,"resolved_dukascopy_code":code,"year":a.year,
      "source_base_url":BASE_URL,"source_transport":"Dukascopy Jetta JSON API","price_side":"BID","timeframe":"M1","timezone":"UTC",
      "dependency_repo":"tomas-charvat/Dukascopy_downloader","dependency_commit":PINNED_DOWNLOADER_COMMIT,"dependency_version":EXPECTED_DOWNLOADER_VERSION,
      "acquisition_script_sha256":sha256_file(pathlib.Path(__file__)),"request_days":len(manifest),
      "http_200_days":sum(x["http_status"]==200 for x in manifest),"http_404_days":sum(x["http_status"]==404 for x in manifest),
      "source_raw_bytes":sum(x["bytes"] for x in manifest),"raw_http_manifest_sha256":sha256_file(mp),
      "normalized_file":outgz.name,"normalized_sha256":sha256_file(outgz),"normalized_rows":len(all_rows),
      "first_timestamp_utc":first.isoformat().replace("+00:00","Z"),"last_timestamp_utc":last.isoformat().replace("+00:00","Z"),
      "bad_ohlc_rows":bad,"duplicate_timestamps":dup,"nonmonotone_timestamps":nonmono,
      "max_observed_gap_seconds":maxgap,"gaps_over_3h":gaps3,
      "decoder_crosscheck":"every HTTP-200 day matched pinned dukascopy_downloader.decode_candles",
      "missing_minutes_policy":"never fabricated; source omissions/closures remain absent"
    }
    apath=root/"AUDIT.json"; apath.write_bytes(canonical(audit))
    (root/"SHA256SUMS.txt").write_text(
      f"{audit['normalized_sha256']}  {outgz.name}\n{audit['raw_http_manifest_sha256']}  RAW_HTTP_MANIFEST.json\n{sha256_file(apath)}  AUDIT.json\n{sha256_file(metadir/'resolved_instrument.json')}  _meta/resolved_instrument.json\n",
      encoding="utf-8")
    print(json.dumps(audit,sort_keys=True))
if __name__=="__main__": main()
