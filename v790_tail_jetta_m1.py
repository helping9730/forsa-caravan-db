#!/usr/bin/env python3
"""V790 sealed-candidate tail acquisition: 2026-10-26 through 2026-11-08 inclusive.
DATA ONLY. V790 strategy was frozen in Drive before this file was created.
Preserves exact Dukascopy/Jetta HTTP response bytes and hashes.
"""
from __future__ import annotations
import argparse,csv,datetime as dt,gzip,hashlib,io,json,math,pathlib,time
import requests
from dukascopy_downloader.client import BASE_URL,DEFAULT_HEADERS,DukascopyClient
from dukascopy_downloader.decode import decode_candles
from dukascopy_downloader.instruments import Instruments
from dukascopy_downloader.timeframes import Chunk
FREEZE_SHA="2f90b92ed52d72a05b1293745710c43dac4a4da9fc7bcb2ee8e92df1629bc7da"
START=dt.date(2026,10,26);END=dt.date(2026,11,9)
def shab(b):return hashlib.sha256(b).hexdigest()
def shaf(p):
 h=hashlib.sha256()
 with open(p,'rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def decs(m):
 x=-math.log10(m);return max(0,int(round(x))) if abs(x-round(x))<1e-9 else 10
def cs(v):
 out=[];s=0
 for x in v:s+=int(x);out.append(s)
 return out
def decode(j,start,end):
 times=j.get("times") or [];n=len(times)
 if not n:return [],decs(float(j.get("multiplier",1e-5)))
 m=float(j["multiplier"]);d=decs(m);shift=int(j.get("shift") or 60000);ms=[int(j["timestamp"])+x*shift for x in cs(times)]
 def ser(b,k):
  base=int(round(float(j[b])/m));return [round((base+x)*m,d) for x in cs(j[k])]
 o,h,l,c=ser("open","opens"),ser("high","highs"),ser("low","lows"),ser("close","closes");v=[float(x) for x in j["volumes"]]
 rows=[(dt.datetime.fromtimestamp(ms[i]/1000,tz=dt.timezone.utc),o[i],h[i],l[i],c[i],v[i]) for i in range(n)]
 ref=decode_candles(j,start=start,end=end)
 if len(ref)!=len(rows):raise RuntimeError("decoder parity rows")
 return rows,d
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--market",required=True);ap.add_argument("--symbol",required=True);ap.add_argument("--out",required=True);a=ap.parse_args()
 root=pathlib.Path(a.out);raw=root/"raw_daily_json";meta=root/"_meta";raw.mkdir(parents=True,exist_ok=True);meta.mkdir(parents=True,exist_ok=True)
 dc=DukascopyClient(rate_limit=1,max_retries=8);ins=Instruments(dc,meta);code=ins.resolve(a.symbol);details=ins.details(code);dc.close()
 (meta/"resolved_instrument.json").write_text(json.dumps({"input":a.symbol,"resolved":code,"details":details},sort_keys=True))
 ses=requests.Session();ses.headers.update(DEFAULT_HEADERS);manifest=[];rows=[];last_req=0.;dec=None
 day=START
 while day<END:
  start=dt.datetime.combine(day,dt.time(),tzinfo=dt.timezone.utc);end=start+dt.timedelta(days=1);url=BASE_URL+Chunk("m1",start,end).url_path(code,"BID")
  wait=max(0,.65-(time.monotonic()-last_req));time.sleep(wait);r=ses.get(url,timeout=(10,60));last_req=time.monotonic()
  if r.status_code not in (200,404):raise RuntimeError(f"{day} HTTP {r.status_code} {r.text[:200]}")
  body=bytes(r.content);ent={"date":day.isoformat(),"status":r.status_code,"bytes":len(body),"sha256":shab(body),"url":url}
  if r.status_code==200:
   (raw/f"{day}.json").write_bytes(body);j=json.loads(body);rr,d=decode(j,start,end);rows.extend(rr);ent["rows"]=len(rr);dec=d if dec is None else dec
  else:(raw/f"{day}.http404.bin").write_bytes(body);ent["rows"]=0
  manifest.append(ent);day+=dt.timedelta(days=1)
 ses.close();rows.sort(key=lambda x:x[0]);dec=dec or 10
 out=root/f"{a.market}_20261026_20261108_M1_BID_JETTA.csv.gz"
 with out.open("wb") as fo:
  with gzip.GzipFile(filename="",mode="wb",fileobj=fo,mtime=0,compresslevel=9) as gz:
   with io.TextIOWrapper(gz,encoding="utf-8",newline="") as txt:
    w=csv.writer(txt,lineterminator="\n");w.writerow(["timestamp_utc","open","high","low","close","volume"])
    for ts,o,h,l,c,v in rows:w.writerow([ts.isoformat().replace("+00:00","Z"),f"{o:.{dec}f}",f"{h:.{dec}f}",f"{l:.{dec}f}",f"{c:.{dec}f}",format(v,".15g")])
 mp=root/"RAW_HTTP_MANIFEST.json";mp.write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
 audit={"contract":"V790_POST_DESIGN_RETRIEVAL_TAIL_V1","v790_freeze_zip_sha256":FREEZE_SHA,"market":a.market,"resolved":code,"start":str(START),"end_exclusive":str(END),"rows":len(rows),"normalized_sha256":shaf(out),"raw_manifest_sha256":shaf(mp),"source":"Dukascopy Jetta M1 BID"}
 (root/"AUDIT.json").write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n");print(json.dumps(audit,sort_keys=True))
if __name__=="__main__":main()
