import os,time,json,sqlite3,logging,statistics,urllib.request,urllib.parse,datetime,math
from concurrent.futures import ThreadPoolExecutor,as_completed
BASE='https://fapi.binance.com'; TOKEN=os.getenv('TELEGRAM_BOT_TOKEN',''); CHAT=os.getenv('TELEGRAM_CHAT_ID',''); DB='/opt/maxel-crypto/signals.db'
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
def get(path,params=None):
 url=BASE+path+('?' + urllib.parse.urlencode(params) if params else '')
 req=urllib.request.Request(url,headers={'User-Agent':'MaxelCryptoScanner/1.0'})
 with urllib.request.urlopen(req,timeout=18) as r:return json.load(r)
def send(message):
 if not TOKEN or not CHAT: logging.warning('Telegram not configured');return
 data=urllib.parse.urlencode({'chat_id':CHAT,'text':message}).encode()
 with urllib.request.urlopen(urllib.request.Request('https://api.telegram.org/bot'+TOKEN+'/sendMessage',data=data),timeout=20) as r: logging.info('Telegram response %s',r.status)
def setup():
 os.makedirs('/opt/maxel-crypto',exist_ok=True)
 with sqlite3.connect(DB) as c:c.execute('CREATE TABLE IF NOT EXISTS signals (symbol TEXT, side TEXT, candle INTEGER, entry REAL, stop REAL, tp REAL, status TEXT DEFAULT "OPEN", created INTEGER, UNIQUE(symbol,side,candle))')
def symbols():
 d=get('/fapi/v1/exchangeInfo');return [s['symbol'] for s in d['symbols'] if s['status']=='TRADING' and s['contractType']=='PERPETUAL' and s['quoteAsset']=='USDT']
def analyse(symbol):
 try:
  raw=get('/fapi/v1/klines',{'symbol':symbol,'interval':'15m','limit':80})
  a=raw[:-1]; closes=[float(x[4]) for x in a]; highs=[float(x[2]) for x in a];lows=[float(x[3]) for x in a];vols=[float(x[5]) for x in a]
  if len(a)<70:return
  ema=lambda seq,n: _ema(seq,n)
  e20=ema(closes,20);e50=ema(closes,50);last=closes[-1];prev=closes[-2]
  diffs=[closes[i]-closes[i-1] for i in range(-14,0)];g=sum(max(0,x) for x in diffs)/14;loss=sum(max(0,-x) for x in diffs)/14;rsi=100 if loss==0 else 100-100/(1+g/loss)
  tr=[max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])) for i in range(-14,0)];atr=sum(tr)/14
  if atr<=0 or last<=0:return
  avgvol=statistics.mean(vols[-21:-1]);volume=vols[-1]/avgvol if avgvol else 0
  breakout_up=last>max(highs[-21:-1]);breakout_down=last<min(lows[-21:-1]);side=None
  if e20>e50 and 52<rsi<72 and breakout_up and volume>1.5:side='LONG'
  if e20<e50 and 28<rsi<48 and breakout_down and volume>1.5:side='SHORT'
  if not side:return
  stop=last-1.5*atr if side=='LONG' else last+1.5*atr;tp=last+3*atr if side=='LONG' else last-3*atr
  dist=abs(last-stop);qty=15/dist;notional=qty*last
  if notional>9000:qty=9000/last;notional=9000
  if notional<5:return
  candle=int(a[-1][0]);return (symbol,side,candle,last,stop,tp,qty,notional,volume,rsi)
 except Exception as e:logging.warning('%s: %s',symbol,e)
def _ema(vals,n):
 k=2/(n+1);out=vals[0]
 for v in vals[1:]:out=v*k+out*(1-k)
 return out
def record(s):
 symbol,side,candle,entry,stop,tp,qty,notional,volume,rsi=s
 with sqlite3.connect(DB) as c:
  cur=c.execute('INSERT OR IGNORE INTO signals(symbol,side,candle,entry,stop,tp,created) VALUES(?,?,?,?,?,?,?)',(symbol,side,candle,entry,stop,tp,int(time.time())))
  if not cur.rowcount:return
 margin=notional/3
 send(f'📊 MAXEL FUTURES SIGNAL (experimental)\n{symbol} {side}\nEntry: {entry:.8g}\nStop: {stop:.8g}\nTake profit: {tp:.8g}\nSuggested quantity: {qty:.6g}\nNotional: {notional:.2f} USDT\nApprox. isolated margin at 3x: {margin:.2f} USDT\nPlanned stop risk: {qty*abs(entry-stop):.2f} USDT, excluding fees/slippage\nRSI: {rsi:.1f} | Volume ratio: {volume:.2f}\n15m breakout + EMA trend. Verify on Binance before trading. Losses can exceed stop risk.')
def main():
 setup();send('✅ Maxel Crypto scanner started. Public Binance Futures market data only; no order execution. Scans all active USDT perpetual pairs on a rolling cycle.')
 last_refresh=0;all_symbols=[];index=0;last_brief=-1
 while True:
  try:
   now=time.time()
   if now-last_refresh>3600 or not all_symbols:
    all_symbols=symbols();last_refresh=now;index=0;logging.info('Tracking %d futures contracts',len(all_symbols))
   batch=all_symbols[index:index+20];index=(index+20)%len(all_symbols)
   with ThreadPoolExecutor(max_workers=4) as pool:
    for f in as_completed([pool.submit(analyse,s) for s in batch]):
     result=f.result()
     if result:record(result)
   utc=datetime.datetime.now(datetime.timezone.utc);perth=utc+datetime.timedelta(hours=8);key=perth.date().toordinal()*10+perth.hour
   if perth.hour in (8,14,20) and perth.minute<3 and key!=last_brief:
    with sqlite3.connect(DB) as c:count=c.execute('SELECT COUNT(*) FROM signals WHERE created>?',(int(now)-86400,)).fetchone()[0]
    send(f'📋 Maxel market update — {perth:%Y-%m-%d %H:%M} Perth\nMonitoring {len(all_symbols)} active USDT perpetual contracts. {count} new qualifying signals recorded in the past 24h. No trade is recommended unless a qualifying alert is generated.')
    last_brief=key
   time.sleep(15)
  except KeyboardInterrupt:break
  except Exception:logging.exception('Scanner cycle failed');time.sleep(60)
if __name__=='__main__':main()
