#!/usr/bin/env python3
"""V801 burned-history ASK acquisition, 2015-2025.

DATA TRANSPORT ONLY. No alpha logic. Preserves exact Dukascopy/Jetta ASK HTTP
responses, cross-checks decoder output, hashes raw/normalized data, and refuses
years outside the already-burned 2015-2025 research block.
"""
from __future__ import annotations
import argparse,csv,datetime as dt,gzip,hashlib,io,json,math,pathlib,time
import requests
from dukascopy_downloader.client import BASE_URL,DEFAULT_HEADERS,DukascopyClient
from dukascopy_downloader.decode import decode_candles
from dukascopy_downloader.instruments import Instruments
from dukascopy_downloader.timeframes import Chunk
PIN="6dc1b07e697e86477721e8d99fcb824fb01d42ab"; VER="1.1.0"
YEARS=set(range(2015,2026)); CONTRACT="V801_BURNED_2015_2025_JETTA_RAW_M1_ASK_V1"
def shab(b): return hashlib.sha256(b).hexdigest()
def shaf(p):
 h=hashlib.sha256()
 with open(p,"rb") as f:
  for z in iter(lambda:f.read(1048576),b""):h.update(z)
 return h.hexdigest()
def canon(x): return (json.dumps(x,sort_keys=True,separators=(",",":"))+"\n").encode()
def dates(y):
 d=dt.date(y,1,1);e=dt.date(y+1,1,1)
 while d<e: yield d; d+=dt.timedelta(days=1)
def decs(m):
 x=-math.log10(m);return max(0,int(round(x))) if abs(x-round(x))<1e-9 else 10
def native(j,start,end):
 ts=j.get("times") or [];n=len(ts)
 if not n:return [],decs(float(j.get("multiplier",1e-5)))
 mult=float(j["multiplier"]);dec=decs(mult);shift=int(j.get("shift") or 60000);t0=int(j["timestamp"])
 def cs(a):
  out=[];s=0
  for v in a:s+=int(v);out.append(s)
  return out
 td=cs(ts); ms=[t0+x*shift for x in td]
 if n>1 and any(int(x)<=0 for x in ts[1:]):raise ValueError("non-increasing timestamp delta")
 if ms[0]<int(start.timestamp()*1000) or ms[-1]>=int(end.timestamp()*1000):raise ValueError("timestamp outside UTC day")
 def ser(base,key):
  a=j[key]
  if len(a)!=n:raise ValueError(key+" length mismatch")
  b=int(round(float(j[base])/mult));return [round((b+x)*mult,dec) for x in cs(a)]
 o=ser("open","opens");h=ser("high","highs");l=ser("low","lows");c=ser("close","closes");v=[float(x) for x in j["volumes"]]
 return [(dt.datetime.fromtimestamp(ms[i]/1000,tz=dt.timezone.utc),o[i],h[i],l[i],c[i],v[i]) for i in range(n)],dec
def parity(j,start,end,rows):
 r=decode_candles(j,start=start,end=end)
 if len(r)!=len(rows):raise AssertionError("decoder length")
 for i,x in enumerate(rows):
  rr=r.iloc[i]
  if x[0]!=rr["time"].to_pydatetime():raise AssertionError("time parity")
  for k,v in zip(("open","high","low","close","volume"),x[1:]):
   if not math.isclose(float(v),float(rr[k]),rel_tol=0,abs_tol=1e-11):raise AssertionError(k+" parity")
class Rate:
 def __init__(self,r):self.dt=1/max(float(r),.1);self.n=0.
 def wait(self):
  x=time.monotonic()
  if self.n>x:time.sleep(self.n-x)
  self.n=max(x,self.n)+self.dt
def fetch(s,url,lim):
 last=None
 for a in range(8):
  lim.wait()
  try:
   r=s.get(url,timeout=(10,60))
   if r.status_code in (200,404):return r.status_code,bytes(r.content),dict(r.headers)
   last=f"HTTP {r.status_code} {r.text[:120]!r}"
   if r.status_code in (202,403,408,425,429) or r.status_code>=500 or r.headers.get("x-amzn-waf-action"):
    time.sleep(min(30.,1.5*(2**a)));continue
   raise RuntimeError(last)
  except (requests.ConnectionError,requests.Timeout,requests.exceptions.ChunkedEncodingError) as e:
   last=repr(e);time.sleep(min(30.,1.5*(2**a)))
 raise RuntimeError("fetch exhausted "+str(last))
def main():
 p=argparse.ArgumentParser();p.add_argument("--market",required=True);p.add_argument("--symbol",required=True);p.add_argument("--year",type=int,required=True);p.add_argument("--out",required=True);p.add_argument("--rate",type=float,default=1.5);a=p.parse_args()
 if a.year not in YEARS:raise SystemExit("REFUSED: V801 ASK transport is burned 2015-2025 only")
 import dukascopy_downloader as duka
 if duka.__version__!=VER:raise SystemExit("dependency drift")
 root=pathlib.Path(a.out);raw=root/"raw_daily_json";meta=root/"_meta";raw.mkdir(parents=True,exist_ok=True);meta.mkdir(parents=True,exist_ok=True)
 dc=DukascopyClient(rate_limit=1,max_retries=8);ins=Instruments(dc,meta);code=ins.resolve(a.symbol);details=ins.details(code);dc.close()
 (meta/"resolved_instrument.json").write_bytes(canon({"input_symbol":a.symbol,"resolved_code":code,"details":details}))
 s=requests.Session();s.headers.update(DEFAULT_HEADERS);lim=Rate(a.rate);manifest=[];allrows=[];dec=None
 for n,day in enumerate(dates(a.year),1):
  st=dt.datetime(day.year,day.month,day.day,tzinfo=dt.timezone.utc);en=st+dt.timedelta(days=1);path=Chunk("m1",st,en).url_path(code,"ASK");status,body,h=fetch(s,BASE_URL+path,lim)
  ent={"date":day.isoformat(),"url_path":path,"http_status":status,"bytes":len(body),"sha256":shab(body)}
  if status==200:
   (raw/f"{day}.json").write_bytes(body);j=json.loads(body);rows,d=native(j,st,en);parity(j,st,en,rows);ent["decoded_rows"]=len(rows);allrows+=rows
   if rows:dec=d if dec is None else dec
  else:(raw/f"{day}.http404.bin").write_bytes(body);ent["decoded_rows"]=0
  manifest.append(ent)
  if n%50==0:print(a.market,a.year,n,flush=True)
 s.close();allrows.sort(key=lambda x:x[0]);mp=root/"RAW_HTTP_MANIFEST.json";mp.write_bytes(canon(manifest))
 if not allrows:raise SystemExit("no rows")
 bad=dup=nonmono=0;prev=None
 for x in allrows:
  ts,o,h,l,c,v=x;bad+=int(min(o,h,l,c)<=0 or h<max(o,c) or l>min(o,c) or l>h)
  if prev is not None:dup+=int(ts==prev);nonmono+=int(ts<prev)
  prev=ts
 if bad or dup or nonmono:raise SystemExit(f"audit failed {bad=} {dup=} {nonmono=}")
 out=root/f"{a.market}_{a.year}_M1_ASK_JETTA.csv.gz"
 with out.open("wb") as f:
  with gzip.GzipFile(filename="",mode="wb",fileobj=f,compresslevel=9,mtime=0) as z:
   with io.TextIOWrapper(z,encoding="utf-8",newline="") as t:
    w=csv.writer(t,lineterminator="\n");w.writerow(["timestamp_utc","open","high","low","close","volume"])
    for ts,o,h,l,c,v in allrows:w.writerow([ts.isoformat().replace("+00:00","Z"),f"{o:.{dec}f}",f"{h:.{dec}f}",f"{l:.{dec}f}",f"{c:.{dec}f}",format(v,".15g")])
 audit={"contract":CONTRACT,"market":a.market,"year":a.year,"side":"ASK","source_transport":"Dukascopy Jetta JSON API","dependency_commit":PIN,"dependency_version":VER,"rows":len(allrows),"normalized_sha256":shaf(out),"raw_manifest_sha256":shaf(mp),"bad_ohlc_rows":bad,"duplicate_timestamps":dup,"nonmonotone_timestamps":nonmono,"acquisition_script_sha256":shaf(pathlib.Path(__file__))}
 ap=root/"AUDIT.json";ap.write_bytes(canon(audit));(root/"SHA256SUMS.txt").write_text(f"{audit['normalized_sha256']}  {out.name}\n{audit['raw_manifest_sha256']}  RAW_HTTP_MANIFEST.json\n{shaf(ap)}  AUDIT.json\n",encoding="utf-8")
 print(json.dumps(audit,sort_keys=True))
if __name__=="__main__":main()
