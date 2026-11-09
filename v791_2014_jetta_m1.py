#!/usr/bin/env python3
"""V791 untouched 2014 transfer raw Dukascopy/Jetta M1 BID acquisition.
DATA TRANSPORT ONLY. No strategy logic. This build refuses every year except 2014.
"""
from __future__ import annotations
import argparse,csv,datetime as dt,gzip,hashlib,io,json,math,pathlib,time
from typing import Any
import requests
from dukascopy_downloader.client import BASE_URL,DEFAULT_HEADERS,DukascopyClient
from dukascopy_downloader.decode import decode_candles
from dukascopy_downloader.instruments import Instruments
from dukascopy_downloader.timeframes import Chunk
PINNED_DOWNLOADER_COMMIT="6dc1b07e697e86477721e8d99fcb824fb01d42ab"
EXPECTED_DOWNLOADER_VERSION="1.1.0"
V791_FREEZE_SHA256="b883c4286104f58eb0a9633c01857fa936568581437639b768c9a1296f734961"
CONTRACT="V791_2014_TRANSFER_JETTA_RAW_M1_BID_V1"
def shab(b): return hashlib.sha256(b).hexdigest()
def shaf(p):
 h=hashlib.sha256()
 with pathlib.Path(p).open("rb") as f:
  for x in iter(lambda:f.read(1024*1024),b""): h.update(x)
 return h.hexdigest()
def canon(x): return (json.dumps(x,sort_keys=True,separators=(",",":"))+"\n").encode()
def days(y):
 d=dt.date(y,1,1);e=dt.date(y+1,1,1)
 while d<e: yield d;d+=dt.timedelta(days=1)
def decs(mult):
 x=-math.log10(float(mult));return max(0,int(round(x))) if abs(x-round(x))<1e-9 else 10
def decode_native(j,start,end):
 times=j.get("times") or [];n=len(times)
 if not n:return [],decs(j.get("multiplier",1e-5))
 mult=float(j["multiplier"]);dec=decs(mult);shift=int(j.get("shift") or 60000);ts0=int(j["timestamp"])
 def cs(v):
  out=[];s=0
  for z in v:s+=int(z);out.append(s)
  return out
 td=cs(times);ms=[ts0+x*shift for x in td];lo=int(start.timestamp()*1000);hi=int(end.timestamp()*1000)
 if n>1 and any(int(x)<=0 for x in times[1:]):raise ValueError("nonincreasing timestamps")
 if ms[0]<lo or ms[-1]>=hi:raise ValueError("timestamp outside UTC day")
 def ser(b,k):
  vals=j[k];base=int(round(float(j[b])/mult));acc=cs(vals)
  if len(vals)!=n:raise ValueError(k+" length mismatch")
  return [round((base+x)*mult,dec) for x in acc]
 o=ser("open","opens");h=ser("high","highs");l=ser("low","lows");c=ser("close","closes");v=[float(x) for x in j["volumes"]]
 return [(dt.datetime.fromtimestamp(ms[i]/1000,tz=dt.timezone.utc),o[i],h[i],l[i],c[i],v[i]) for i in range(n)],dec
def parity(j,start,end,rows):
 ref=decode_candles(j,start=start,end=end)
 if len(ref)!=len(rows):raise AssertionError("decoder length mismatch")
 for i,row in enumerate(rows):
  rr=ref.iloc[i]
  if row[0]!=rr["time"].to_pydatetime():raise AssertionError("time parity")
  for k,v in zip(("open","high","low","close","volume"),row[1:]):
   if not math.isclose(float(v),float(rr[k]),rel_tol=0,abs_tol=1e-11):raise AssertionError(k+" parity")
class Rate:
 def __init__(self,r):self.dt=1/max(float(r),0.1);self.next=0
 def wait(self):
  now=time.monotonic()
  if self.next>now:time.sleep(self.next-now)
  self.next=max(now,self.next)+self.dt
def fetch(s,url,lim,n=8):
 last=None
 for a in range(1,n+1):
  lim.wait()
  try:
   r=s.get(url,timeout=(10,60));st=r.status_code
   if st in (200,404):return st,bytes(r.content),dict(r.headers)
   if st==400:raise RuntimeError("HTTP400 "+r.text[:200])
   last=f"HTTP {st} {r.text[:160]!r}"
   if st in (202,403,408,425,429) or st>=500 or r.headers.get("x-amzn-waf-action"):
    time.sleep(min(30,1.5*(2**(a-1))));continue
   raise RuntimeError(last)
  except (requests.ConnectionError,requests.Timeout,requests.exceptions.ChunkedEncodingError) as e:
   last=str(e);time.sleep(min(30,1.5*(2**(a-1))))
 raise RuntimeError("fetch exhausted "+str(last))
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--market",required=True);ap.add_argument("--symbol",required=True);ap.add_argument("--year",type=int,required=True);ap.add_argument("--out",required=True);ap.add_argument("--rate",type=float,default=1.5);a=ap.parse_args()
 if a.year!=2014:raise SystemExit("TRANSFER REFUSED: this build only permits 2014")
 import dukascopy_downloader as duka
 if duka.__version__!=EXPECTED_DOWNLOADER_VERSION:raise SystemExit("dependency drift")
 root=pathlib.Path(a.out);raw=root/"raw_daily_json";meta=root/"_meta";raw.mkdir(parents=True,exist_ok=True);meta.mkdir(parents=True,exist_ok=True)
 dc=DukascopyClient(rate_limit=1,max_retries=8);inst=Instruments(dc,meta);code=inst.resolve(a.symbol);details=inst.details(code);dc.close()
 (meta/"resolved_instrument.json").write_bytes(canon({"input_symbol":a.symbol,"resolved_code":code,"details":details}))
 ses=requests.Session();ses.headers.update(DEFAULT_HEADERS);lim=Rate(a.rate);manifest=[];rows=[];dec=None
 for i,day in enumerate(days(a.year),1):
  start=dt.datetime(day.year,day.month,day.day,tzinfo=dt.timezone.utc);end=start+dt.timedelta(days=1);path=Chunk("m1",start,end).url_path(code,"BID");st,body,hdr=fetch(ses,BASE_URL+path,lim)
  ent={"date":day.isoformat(),"url_path":path,"http_status":st,"bytes":len(body),"sha256":shab(body),"content_type":hdr.get("Content-Type")}
  if st==200:
   (raw/f"{day}.json").write_bytes(body);j=json.loads(body);rr,d=decode_native(j,start,end);parity(j,start,end,rr);dec=d if dec is None else dec
   if rr and d!=dec:raise RuntimeError("decimal drift")
   ent["decoded_rows"]=len(rr);rows.extend(rr)
  else:(raw/f"{day}.http404.bin").write_bytes(body);ent["decoded_rows"]=0
  manifest.append(ent)
  if i%50==0:print(a.market,i,"days",flush=True)
 ses.close();rows.sort(key=lambda x:x[0])
 if not rows:raise SystemExit("no rows")
 bad=dup=nonmono=0;prev=None
 for ts,o,h,l,c,v in rows:
  bad+=int(min(o,h,l,c)<=0 or h<max(o,c) or l>min(o,c) or l>h)
  if prev is not None:dup+=int(ts==prev);nonmono+=int(ts<prev)
  prev=ts
 if bad or dup or nonmono:raise SystemExit(f"audit fail {bad=} {dup=} {nonmono=}")
 outgz=root/f"{a.market}_2014_M1_BID_JETTA.csv.gz"
 with outgz.open("wb") as rawf:
  with gzip.GzipFile(filename="",mode="wb",fileobj=rawf,compresslevel=9,mtime=0) as gz:
   with io.TextIOWrapper(gz,encoding="utf-8",newline="") as txt:
    w=csv.writer(txt,lineterminator="\n");w.writerow(["timestamp_utc","open","high","low","close","volume"])
    for ts,o,h,l,c,v in rows:w.writerow([ts.isoformat().replace("+00:00","Z"),f"{o:.{dec}f}",f"{h:.{dec}f}",f"{l:.{dec}f}",f"{c:.{dec}f}",format(v,".15g")])
 mp=root/"RAW_HTTP_MANIFEST.json";mp.write_bytes(canon(manifest))
 audit={"contract":CONTRACT,"v791_freeze_sha256":V791_FREEZE_SHA256,"market":a.market,"year":2014,"resolved_dukascopy_code":code,"source_base_url":BASE_URL,"source_transport":"Dukascopy Jetta JSON API","price_side":"BID","timeframe":"M1","timezone":"UTC","dependency_commit":PINNED_DOWNLOADER_COMMIT,"dependency_version":EXPECTED_DOWNLOADER_VERSION,"request_days":len(manifest),"http_200_days":sum(x["http_status"]==200 for x in manifest),"http_404_days":sum(x["http_status"]==404 for x in manifest),"normalized_file":outgz.name,"normalized_sha256":shaf(outgz),"normalized_rows":len(rows),"bad_ohlc_rows":bad,"duplicate_timestamps":dup,"nonmonotone_timestamps":nonmono,"raw_http_manifest_sha256":shaf(mp),"acquisition_script_sha256":shaf(pathlib.Path(__file__))}
 apath=root/"AUDIT.json";apath.write_bytes(canon(audit));(root/"SHA256SUMS.txt").write_text(f"{audit['normalized_sha256']}  {outgz.name}\n{audit['raw_http_manifest_sha256']}  RAW_HTTP_MANIFEST.json\n{shaf(apath)}  AUDIT.json\n{shaf(meta/'resolved_instrument.json')}  _meta/resolved_instrument.json\n")
 print(json.dumps(audit,sort_keys=True))
if __name__=="__main__":main()
