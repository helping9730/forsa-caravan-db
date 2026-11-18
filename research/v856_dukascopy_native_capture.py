#!/usr/bin/env python3
"""STARK/V856 research-only missing native Dukascopy minute capture. NO TRADING.
Seven original 2026 source instruments; both BID/ASK; every US trading day Nov11-17.
Outputs provider-original bytes, SHA256 and complete attempt ledger. Fail closed.
"""
import concurrent.futures, datetime as dt, hashlib, json, lzma, pathlib, struct, time, urllib.request
ROOT=pathlib.Path("v856_provider_raw_20261118"); ROOT.mkdir(exist_ok=True)
DAYS=("2026-11-11","2026-11-12","2026-11-13","2026-11-16","2026-11-17")
SYMS={"US500":"USA500.IDX-USD","USTECH100":"USATECH.IDX-USD","GERMANY40":"DEU.IDX-EUR",
      "UK100":"GBR.IDX-GBP","WTI":"LIGHT.CMD-USD","EURUSD":"EUR-USD","BTCUSD":"BTC-USD"}
HEADERS={"User-Agent":"Mozilla/5.0 (compatible; frozen-V856-market-research/1.0)","Accept":"application/json,application/octet-stream,*/*","Referer":"https://www.dukascopy.com/"}
def urls(inst,side,date):
 y,m,d=map(int,date.split("-"));sym=inst.replace(".","").replace("-","")
 return [("jetta",f"https://jetta.dukascopy.com/v1/candles/minute/{inst}/{side}/{y}/{m}/{d}"),
         ("bi5",f"https://datafeed.dukascopy.com/datafeed/{sym}/{y}/{m-1:02d}/{d:02d}/{side}_candles_min_1.bi5")]
def validity(label,body):
 if not body or len(body)>8000000:return False,"EMPTY_OR_TOO_LARGE"
 if label=="bi5":
  try:
   p=lzma.decompress(body)
   if len(p)%24 or len(p)<24*180:return False,"BAD_BI5_BYTES"
   ts=[struct.unpack_from(">I",p,i)[0] for i in range(0,len(p),24)]
   return ts==sorted(ts) and len(set(ts))==len(ts),f"BI5_CANDLES={len(ts)}"
  except Exception as e:return False,"BI5_DECODE_ERROR:"+str(e)[:90]
 try:
  obj=json.loads(body)
  n=len(obj["times"])
  if n<180 or not all(len(obj[k])==n for k in ("opens","highs","lows","closes","volumes")):
   return False,"BAD_JSON_LENGTH"
  if obj["shift"]!=60000 or not all(k in obj for k in ("timestamp","multiplier","open","high","low","close")):
   return False,"BAD_JSON_FIELDS"
  return True,f"JETTA_CANDLES={n}"
 except Exception as e:return False,"JSON_PARSE_ERROR:"+str(e)[:100]
def one(request):
 date,market,side=request; attempts=[];chosen=None
 for label,url in urls(SYMS[market],side,date):
  info={"date":date,"market":market,"instrument":SYMS[market],"side":side,"method":label,"url":url,
        "utc_time":dt.datetime.now(dt.timezone.utc).isoformat()}
  try:
   with urllib.request.urlopen(urllib.request.Request(url,headers=HEADERS),timeout=16) as resp:
    raw=resp.read(8000001);info["http_status"]=resp.status;info["content_type"]=resp.headers.get("Content-Type")
   info["size_bytes"]=len(raw);info["sha256"]=hashlib.sha256(raw).hexdigest()
   ok,msg=validity(label,raw);info["valid"]=ok;info["validation"]=msg
   if ok:
    fn=f"{date}_{market}_{side}_{label}."+("json" if label=="jetta" else "bi5")
    (ROOT/fn).write_bytes(raw);info["saved_filename"]=fn;chosen=fn
  except Exception as e:info["valid"]=False;info["error"]=repr(e)[:260]
  attempts.append(info)
  if chosen:break
 return {"date":date,"market":market,"side":side,"saved_filename":chosen,"attempts":attempts}
tasks=[(date,m,s) for date in DAYS for m in SYMS for s in ("BID","ASK")]
print(f"REQUESTING={len(tasks)} native quote files",flush=True)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
 results=list(executor.map(one,tasks))
 attempts=[a for item in results for a in item["attempts"]]
 for item in results:
  print(json.dumps({"date":item["date"],"market":item["market"],"side":item["side"],"saved":item["saved_filename"]}),flush=True)
pair={}
for d in DAYS:
 for m in SYMS:
  pair[f"{d}_{m}"]=all(any(x["date"]==d and x["market"]==m and x["side"]==side and x["saved_filename"] for x in results) for side in ("BID","ASK"))
manifest={"research_only":True,"strategy":"V856 immutable", "capital_authority":0,"no_orders":True,
          "started_contract":"V860 frozen 2026-11-11; all earlier history burned",
          "range":list(DAYS),"markets":SYMS,"expected_market_side_days":len(tasks),
          "saved_market_side_days":sum(bool(x["saved_filename"]) for x in results),
          "complete_market_days":sum(pair.values()),"complete_pair_flags":pair,"attempts":attempts,
          "result_not_equivalent_to_V856_portfolio":True}
(ROOT/"SOURCE_RECEIPT_MANIFEST.json").write_text(json.dumps(manifest,indent=2,sort_keys=True))
(ROOT/"ACQUISITION_SUMMARY.json").write_text(json.dumps({"requested":len(tasks),"saved":manifest["saved_market_side_days"],"complete_pairs":sum(pair.values()),"needed_pairs":len(pair),"missing":[k for k,v in pair.items() if not v]},indent=2))
(ROOT/"SHA256SUMS.txt").write_text("".join(hashlib.sha256(p.read_bytes()).hexdigest()+"  "+p.name+"\n" for p in sorted(ROOT.iterdir()) if p.is_file() and p.name!="SHA256SUMS.txt"))
print("ACQUISITION_COMPLETE",sum(pair.values()),"/",len(pair),"paired market-days",flush=True)
