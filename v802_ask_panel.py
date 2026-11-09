#!/usr/bin/env python3
"""V802 native ASK M1 acquisition for burned spread diagnostics only.

DATA TRANSPORT ONLY. No alpha logic. Preserves exact Dukascopy/Jetta ASK
response bytes, hashes them, independently decodes M1 ASK OHLCV and cross-checks
against pinned dukascopy_downloader. Refuses years outside the predeclared burned
spread panel.
"""
from __future__ import annotations
import argparse,csv,datetime as dt,gzip,hashlib,io,json,math,pathlib,time
import requests
from dukascopy_downloader.client import BASE_URL,DEFAULT_HEADERS,DukascopyClient
from dukascopy_downloader.decode import decode_candles
from dukascopy_downloader.instruments import Instruments
from dukascopy_downloader.timeframes import Chunk

PINNED_COMMIT="6dc1b07e697e86477721e8d99fcb824fb01d42ab"
EXPECTED_VERSION="1.1.0"
ALLOWED={2018,2020,2022,2024,2025}
V801_REPORT_SHA256="197b9169cff7d316af28d5fb920dc384a9996361e3651ed2b49a2ea62697c7fd"
CONTRACT="V802_NATIVE_ASK_SPREAD_PANEL_BURNED_ONLY_V1"

def shab(b): return hashlib.sha256(b).hexdigest()
def shaf(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for x in iter(lambda:f.read(1<<20),b""): h.update(x)
    return h.hexdigest()
def canon(x): return (json.dumps(x,sort_keys=True,separators=(",",":"))+"\n").encode()
def dates(y):
    d=dt.date(y,1,1); e=dt.date(y+1,1,1)
    while d<e: yield d; d+=dt.timedelta(days=1)
def decs(m):
    x=-math.log10(m); return max(0,int(round(x))) if abs(x-round(x))<1e-9 else 10
def cs(a):
    s=0; out=[]
    for v in a: s+=int(v); out.append(s)
    return out
def decode(j,start,end):
    t=j.get("times") or []; n=len(t)
    if not n:return [],decs(float(j.get("multiplier",1e-5)))
    mult=float(j["multiplier"]); dec=decs(mult); shift=int(j.get("shift") or 60000)
    td=cs(t); ms=[int(j["timestamp"])+x*shift for x in td]
    lo=int(start.timestamp()*1000); hi=int(end.timestamp()*1000)
    if ms[0]<lo or ms[-1]>=hi: raise ValueError("timestamp outside day")
    def ser(base,key):
        v=j[key]
        if len(v)!=n: raise ValueError(key+" length mismatch")
        b=int(round(float(j[base])/mult))
        return [round((b+x)*mult,dec) for x in cs(v)]
    o,h,l,c=ser("open","opens"),ser("high","highs"),ser("low","lows"),ser("close","closes")
    vol=[float(x) for x in j["volumes"]]
    return [(dt.datetime.fromtimestamp(ms[i]/1000,tz=dt.timezone.utc),o[i],h[i],l[i],c[i],vol[i]) for i in range(n)],dec
def check(j,start,end,rows):
    ref=decode_candles(j,start=start,end=end)
    assert len(ref)==len(rows)
    for i,row in enumerate(rows):
        rr=ref.iloc[i]
        assert row[0]==rr["time"].to_pydatetime()
        for k,v in zip(("open","high","low","close","volume"),row[1:]):
            assert math.isclose(float(v),float(rr[k]),rel_tol=0,abs_tol=1e-11)
class Rate:
    def __init__(self,r):self.dt=1/max(.1,float(r));self.n=0.
    def wait(self):
        now=time.monotonic()
        if self.n>now:time.sleep(self.n-now)
        self.n=max(now,self.n)+self.dt
def fetch(s,url,rate,tries=8):
    last=None
    for a in range(tries):
        rate.wait()
        try:
            r=s.get(url,timeout=(10,60))
            if r.status_code in (200,404): return r.status_code,bytes(r.content),dict(r.headers)
            last=f"HTTP {r.status_code} {r.text[:160]!r}"
            if r.status_code in (202,403,408,425,429) or r.status_code>=500 or r.headers.get("x-amzn-waf-action"):
                time.sleep(min(30,1.5*(2**a)));continue
            raise RuntimeError(last)
        except (requests.ConnectionError,requests.Timeout,requests.exceptions.ChunkedEncodingError) as e:
            last=str(e);time.sleep(min(30,1.5*(2**a)))
    raise RuntimeError("fetch exhausted "+str(last))
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--market",required=True);ap.add_argument("--symbol",required=True)
    ap.add_argument("--year",type=int,required=True);ap.add_argument("--out",required=True)
    ap.add_argument("--rate",type=float,default=1.5)
    a=ap.parse_args()
    if a.year not in ALLOWED: raise SystemExit("REFUSED: burned spread panel years only")
    import dukascopy_downloader as d
    assert d.__version__==EXPECTED_VERSION
    root=pathlib.Path(a.out);raw=root/"raw_daily_json";meta=root/"_meta"
    raw.mkdir(parents=True,exist_ok=True);meta.mkdir(parents=True,exist_ok=True)
    dc=DukascopyClient(rate_limit=1,max_retries=8);ins=Instruments(dc,meta)
    code=ins.resolve(a.symbol);details=ins.details(code);dc.close()
    (meta/"resolved_instrument.json").write_bytes(canon({"input":a.symbol,"code":code,"details":details}))
    s=requests.Session();s.headers.update(DEFAULT_HEADERS);rate=Rate(a.rate)
    man=[];rows=[];dec=None
    for k,day in enumerate(dates(a.year),1):
        st=dt.datetime(day.year,day.month,day.day,tzinfo=dt.timezone.utc);en=st+dt.timedelta(days=1)
        path=Chunk("m1",st,en).url_path(code,"ASK");url=BASE_URL+path
        status,body,headers=fetch(s,url,rate)
        ent={"date":day.isoformat(),"url_path":path,"http_status":status,"bytes":len(body),"sha256":shab(body)}
        if status==200:
            (raw/f"{day.isoformat()}.json").write_bytes(body)
            j=json.loads(body);rr,dd=decode(j,st,en);check(j,st,en,rr)
            dec=dd if dec is None else dec
            if rr and dd!=dec:raise RuntimeError("decimal drift")
            rows.extend(rr);ent["decoded_rows"]=len(rr)
        else:
            (raw/f"{day.isoformat()}.http404.bin").write_bytes(body);ent["decoded_rows"]=0
        man.append(ent)
        if k%50==0:print(a.market,a.year,k,flush=True)
    s.close();rows.sort(key=lambda x:x[0])
    if not rows: raise SystemExit("no ASK rows")
    bad=dup=nonmono=0;prev=None
    for ts,o,h,l,c,v in rows:
        bad+=int(min(o,h,l,c)<=0 or h<max(o,c) or l>min(o,c) or l>h)
        if prev is not None:dup+=int(ts==prev);nonmono+=int(ts<prev)
        prev=ts
    if bad or dup or nonmono:raise SystemExit(f"audit {bad=} {dup=} {nonmono=}")
    out=root/f"{a.market}_{a.year}_M1_ASK_JETTA.csv.gz"
    with out.open("wb") as ro:
      with gzip.GzipFile(filename="",mode="wb",fileobj=ro,compresslevel=9,mtime=0) as gz:
       with io.TextIOWrapper(gz,encoding="utf-8",newline="") as txt:
        w=csv.writer(txt,lineterminator="\n");w.writerow(["timestamp_utc","open","high","low","close","volume"])
        for ts,o,h,l,c,v in rows:w.writerow([ts.isoformat().replace("+00:00","Z"),f"{o:.{dec}f}",f"{h:.{dec}f}",f"{l:.{dec}f}",f"{c:.{dec}f}",format(v,".15g")])
    mp=root/"RAW_HTTP_MANIFEST.json";mp.write_bytes(canon(man))
    audit={"contract":CONTRACT,"v801_report_sha256":V801_REPORT_SHA256,"market":a.market,"year":a.year,"side":"ASK","source_base_url":BASE_URL,"dependency_commit":PINNED_COMMIT,"rows":len(rows),"normalized_sha256":shaf(out),"raw_manifest_sha256":shaf(mp),"bad_ohlc":bad,"duplicates":dup,"nonmonotone":nonmono}
    aa=root/"AUDIT.json";aa.write_bytes(canon(audit))
    (root/"SHA256SUMS.txt").write_text(f"{shaf(out)}  {out.name}\n{shaf(mp)}  RAW_HTTP_MANIFEST.json\n{shaf(aa)}  AUDIT.json\n{shaf(meta/'resolved_instrument.json')}  _meta/resolved_instrument.json\n")
    print(json.dumps(audit,sort_keys=True))
if __name__=="__main__":main()
