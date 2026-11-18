#!/usr/bin/env python3
"""Vendor-original Dukascopy tick-hour capture for already frozen price-path diagnostics.
Read-only; no trading, no signal generation, no retuning, no order placement.
"""
import concurrent.futures,datetime as dt, hashlib,json,lzma,pathlib,struct,urllib.request,time
from collections import defaultdict
ROOT=pathlib.Path("probe_native_ticks");ROOT.mkdir(exist_ok=True)
PEAKS={
"GERMANY40": "2026-04-17/14:43,2026-04-24/11:32,2026-05-06/11:09,2026-05-18/12:45,2026-05-21/13:40,2026-06-01/14:00,2026-06-12/13:41,2026-06-16/08:32,2026-06-18/09:01,2026-06-26/12:00,2026-07-02/13:48,2026-07-14/14:34,2026-09-01/09:41,2026-09-03/15:19,2026-09-09/13:42,2026-09-10/15:13,2026-09-14/09:24,2026-09-23/11:41,2026-09-28/08:32,2026-09-30/11:21,2026-10-08/10:34,2026-11-06/12:02",
"UK100": "2026-04-21/15:05,2026-04-24/11:32,2026-04-28/12:39,2026-04-30/11:50,2026-06-08/11:15,2026-06-18/09:00,2026-06-23/10:23,2026-07-08/12:56,2026-07-14/14:34,2026-07-17/11:10,2026-09-01/08:48,2026-09-03/13:30,2026-09-09/12:18,2026-09-11/13:03,2026-09-17/15:18,2026-09-18/11:19,2026-10-01/12:36,2026-10-05/12:22,2026-10-07/10:54,2026-10-08/08:30,2026-10-08/13:51,2026-11-04/12:03,2026-11-04/15:44,2026-11-06/12:01,2026-11-10/14:54",
"US500": "2026-06-09/16:40,2026-07-29/18:55,2026-09-03/18:01,2026-09-24/15:32,2026-09-28/14:55,2026-10-21/18:16,2026-11-05/16:40,2026-11-17/15:54",
"USTECH100": "2026-04-23/17:47,2026-05-12/16:58,2026-06-09/16:55,2026-06-10/16:00,2026-06-12/17:22,2026-06-29/17:13,2026-07-02/16:09,2026-07-07/15:40,2026-07-14/15:32,2026-07-17/17:11,2026-07-20/19:55,2026-07-23/16:39,2026-07-27/16:26,2026-07-28/16:53,2026-07-29/19:02,2026-07-31/17:38,2026-09-03/18:17,2026-09-28/14:48,2026-10-15/17:42,2026-10-19/18:35,2026-10-21/17:21,2026-11-05/16:35,2026-11-10/16:52,2026-11-12/16:20,2026-11-13/19:21,2026-11-17/15:54"}
INST={"GERMANY40":"DEUIDXEUR","UK100":"GBRIDXGBP","US500":"USA500IDXUSD","USTECH100":"USATECHIDXUSD"}
tasks=set();pairs=[]
for m,v in PEAKS.items():
 for t in v.split(","):
  hour=dt.datetime.strptime(t,"%Y-%m-%d/%H:%M").replace(tzinfo=dt.timezone.utc)
  tasks.add((m,hour));pairs.append((m,hour))
  if hour.minute>=50:tasks.add((m,hour.replace(minute=0)+dt.timedelta(hours=1)))
  if hour.minute<5:tasks.add((m,hour.replace(minute=0)-dt.timedelta(hours=1)))
tasks=sorted({(m,t.replace(minute=0,second=0,microsecond=0)) for m,t in tasks})
assert len(pairs)==81
HEAD={"User-Agent":"Mozilla/5.0","Accept":"application/octet-stream,application/json,*/*","Referer":"https://www.dukascopy.com/"}
def collect(item):
 market,t=item
 sym=INST[market];y=t.year;month=t.month-1;day=t.day;hr=t.hour
 opts=[("www_bi5",f"https://www.dukascopy.com/datafeed/{sym}/{y}/{month:02}/{day:02}/{hr:02}h_ticks.bi5"),
       ("jetta_json",f"https://jetta.dukascopy.com/v1/ticks/{ {'GERMANY40':'DEU.IDX-EUR','UK100':'GBR.IDX-GBP','US500':'USA500.IDX-USD','USTECH100':'USATECH.IDX-USD'}[market]}/{y}/{t.month}/{day}/{hr}")]
 attempts=[];accepted=None
 for fmt,url in opts:
  info={"market":market,"utc_hour":t.isoformat(),"method":fmt,"url":url}
  try:
   req=urllib.request.Request(url,headers=HEAD)
   with urllib.request.urlopen(req,timeout=35) as response:
    raw=response.read(16000001);info["http"]=response.status
   info["bytes"]=len(raw);info["sha256"]=hashlib.sha256(raw).hexdigest()
   if fmt=="www_bi5":
    dec=lzma.decompress(raw);n=len(dec)//20
    if not dec or len(dec)%20 or n<50:raise ValueError("Bad or empty tick binary")
    first=struct.unpack_from(">IIIff",dec,0);last=struct.unpack_from(">IIIff",dec,len(dec)-20)
    if not (0<=first[0]<3600000 and 0<=last[0]<3600000 and first[0]<=last[0]):raise ValueError("Tick time offset invalid")
   else:
    obj=json.loads(raw);n=len(obj["times"])
    if n<50 or any(len(obj[k])!=n for k in ("asks","bids","askVolumes","bidVolumes")):raise ValueError("Bad tick JSON")
   f=f"{market}_{t.strftime('%Y%m%dT%H')}_ticks."+("bi5" if fmt=="www_bi5" else "json")
   (ROOT/f).write_bytes(raw);info.update(valid=True,record_count=n,saved_filename=f);accepted=f
  except Exception as e:info.update(valid=False,error=repr(e)[:160])
  attempts.append(info)
  if accepted:break
 return {"market":market,"utc_hour":t.isoformat(),"saved":accepted,"attempts":attempts}
print("CAPTURE_REQUESTED",len(tasks),"tick hours for",len(pairs),"trades",flush=True)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor: result=list(executor.map(collect,tasks))
all_attempts=[a for row in result for a in row['attempts']]
m={"intent":"Original quote-response evidence only; no score or trading changes","trades_covered":len(pairs),"requested_hour_files":len(tasks),
  "saved_hour_files":sum(bool(x['saved']) for x in result),"missing": [x for x in result if not x["saved"]],
  "requested": result,"peak_hours":[{"market":m,"utc_hour":h.isoformat()} for m,h in pairs]}
(ROOT/"TICK_SOURCE_RECEIPTS.json").write_text(json.dumps(m,indent=2))
(ROOT/"SHA256SUMS.txt").write_text("".join(hashlib.sha256(p.read_bytes()).hexdigest()+"  "+p.name+"\n" for p in sorted(ROOT.iterdir()) if p.is_file() and p.name!="SHA256SUMS.txt"))
print("FINISHED",m['saved_hour_files'],"/",len(tasks),"hour files",flush=True)
