import urllib.request, hashlib, lzma, struct, pathlib, json, datetime
OUT=pathlib.Path("probe_native_ticks");OUT.mkdir(exist_ok=True)
cases=[("US500","USA500IDXUSD","2026-07-29","18"),("USTECH100","USATECHIDXUSD","2026-11-17","15"),("GERMANY40","DEUIDXEUR","2026-07-14","14")]
R=[]
for market,symbol,date,hr in cases:
 y,m,d=map(int,date.split("-"));url=f"https://datafeed.dukascopy.com/datafeed/{symbol}/{y}/{m-1:02}/{d:02}/{hr}h_ticks.bi5"
 info={"market":market,"date":date,"hour":hr,"url":url}
 try:
  req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"})
  with urllib.request.urlopen(req,timeout=25) as resp:
   raw=resp.read(25000001);info["http"]=resp.status;info["type"]=resp.headers.get("Content-Type")
  info["bytes"]=len(raw);info["sha256"]=hashlib.sha256(raw).hexdigest()
  dec=lzma.decompress(raw);info["decoded"]=len(dec);info["divisible_by_20"]=len(dec)%20==0
  if info["divisible_by_20"] and len(dec):
   info["first_record"]=struct.unpack(">IIIff",dec[:20]);info["last_record"]=struct.unpack(">IIIff",dec[-20:])
   (OUT/f"{date}_{market}_{hr}h_ticks.bi5").write_bytes(raw)
 except Exception as e:info["error"]=repr(e)[:300]
 R.append(info);print(json.dumps(info),flush=True)
(OUT/"TICK_PROBE_RECEIPTS.json").write_text(json.dumps(R,indent=2))
