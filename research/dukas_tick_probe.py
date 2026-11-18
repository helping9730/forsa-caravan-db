import urllib.request,hashlib,lzma,struct,pathlib,json,datetime
OUT=pathlib.Path("probe_native_ticks");OUT.mkdir(exist_ok=True)
cases=[("US500","USA500.IDX-USD","2026-07-29","18"),("USTECH100","USATECH.IDX-USD","2026-11-17","15"),("GERMANY40","DEU.IDX-EUR","2026-07-14","14")]
R=[]
for market,instrument,date,hr in cases:
 y,m,d=map(int,date.split("-"))
 methods=[("jetta",f"https://jetta.dukascopy.com/v1/ticks/{instrument}/{y}/{m}/{d}/{int(hr)}"),
          ("dukascopy_www",f"https://www.dukascopy.com/datafeed/{instrument.replace('.','').replace('-','')}/{y}/{m-1:02}/{d:02}/{hr}h_ticks.bi5")]
 for label,url in methods:
  info={"market":market,"date":date,"hour":hr,"method":label,"url":url}
  try:
   req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Referer":"https://www.dukascopy.com/","Accept":"*/*"})
   with urllib.request.urlopen(req,timeout=26) as resp:
    raw=resp.read(25000001);info["http"]=resp.status;info["type"]=resp.headers.get("Content-Type")
   info["bytes"]=len(raw);info["sha256"]=hashlib.sha256(raw).hexdigest()
   if label=="jetta":
    info["start"]=raw[:200].decode(errors="replace")
    try:
     j=json.loads(raw);info["json_type"]=type(j).__name__
     if isinstance(j,dict):info["keys"]=list(j.keys());info["lengths"]={k:len(v) for k,v in j.items() if isinstance(v,list)}
     if isinstance(j,list):info["len"]=len(j)
    except Exception as e:info["json_error"]=str(e)[:130]
    (OUT/f"{date}_{market}_{hr}h_ticks_jetta.json").write_bytes(raw)
   else:
    dec=lzma.decompress(raw);info["decoded"]=len(dec);info["div20"]=len(dec)%20==0
    if info["div20"] and len(dec):info["first_record"]=struct.unpack(">IIIff",dec[:20])
    (OUT/f"{date}_{market}_{hr}h_ticks.bi5").write_bytes(raw)
  except Exception as e:info["error"]=repr(e)[:200]
  R.append(info);print(json.dumps(info),flush=True)
(OUT/"TICK_PROBE_RECEIPTS.json").write_text(json.dumps(R,indent=2))
