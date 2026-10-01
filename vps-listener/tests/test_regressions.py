"""Offline safety regressions. No exchange requests, credentials or orders."""
from __future__ import annotations
import os,sys,tempfile,asyncio,json,copy
from datetime import datetime,timezone
os.environ['DRY_RUN']='false';os.environ['WEBHOOK_SHARED_SECRET']='testsecret'
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import listener,source_api,bybit_api
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
from test_rules import FakeClient,pos,make_book
listener.STATE_FILE=os.path.join(tempfile.mkdtemp(),'state.json')
listener.LOG_FILE=listener.STATE_FILE+'.log'
PASS=0
def ok(cond,label):
 global PASS
 assert cond,label
 PASS+=1;print('  ok - '+label)
def empty():
 return {'mirrored':{},'manual':[],'books':{},'baseline':[],'cutover_armed':True,'hold_new':False,'fresh_start_at':datetime.now(timezone.utc).isoformat(),'manual_adopted':True}
# Exact owner allocation target; denominator/fee noise cannot change it.
p=pos(ticker='SOL',side='long',ep=100,cp=100,lev=10,alloc_pct=1)
c=FakeClient(wallet=400,avail=400);c.ticker_prices['SOLUSDT']=100
m,e=listener.compute_desired_margin(p,{'source_equity':1},400)
ok(m==4 and not e,'1% of 400 gives 4 USDT target margin')
m2,e2=listener.compute_desired_margin(p,{'source_equity':999999999},400)
ok(m2==m and not e2,'source inferred-equity denominator does not affect sizing')
u=copy.deepcopy(p);u['allocation_verified']=False
ok(listener.compute_desired_margin(u,{'source_equity':100},400)[0]==0,'unverified declaration cannot use inferred fallback')
c.live_positions=[{'symbol':'SOLUSDT','size':'1','side':'Sell'}]
a,r=listener.open_mirror(c,'booobsas',p,make_book([p])['booobsas'],empty())
ok(r is None and not c.orders,'opposite manual one-way position prevents netting order')
class Unreachable(FakeClient):
 def get_positions(self,symbol=None):raise bybit_api.BybitError('read failed')
a,r=listener.open_mirror(Unreachable(),'booobsas',p,make_book([p])['booobsas'],empty())
ok(r is None and a.startswith('ERROR'),'failed ownership check blocks new order')
# Pure price/PnL changes never change source quantity.
meta={'success':True,'portfolio':{'id':source_api.PORTFOLIO_ID,'owner':{'username':'booobsas'},'openPositions':1}}
raw={'baseId':'s1','portfolio':{'id':source_api.PORTFOLIO_ID},'isOpen':True,'entrySim':100,'entrySize':1,'leverage':10,'entryPrice':100,'currentPrice':101,'directionLong':True,'ticker':'SOL'}
sims={'success':True,'investments':[{'baseId':'s1','entrySim':100,'lastSim':110}],'portfolioRemainingSim':9900}
b1=source_api.normalize_source(meta,[raw],sims)
raw2=copy.deepcopy(raw);raw2['currentPrice']=102;sims2=copy.deepcopy(sims);sims2['investments'][0]['lastSim']=120
b2=source_api.normalize_source(meta,[raw2],sims2)
ok(b1['positions'][0]['source_qty']==b2['positions'][0]['source_qty'],'PnL-only change leaves stable source qty unchanged')
ok(listener.compute_deltas({'booobsas':b1},{'booobsas':b2})==[],'PnL-only change generates no trade delta')
try:source_api.normalize_source(meta,[],sims)
except source_api.SourceDataError:ok(True,'incomplete source count refuses closure inference')
else:raise AssertionError('incomplete count accepted')
# Real API wrapper tested with fake transport: accepted is not filled.
a=bybit_api.BybitClient(api_key='',api_secret='');writes=[]
a._post=lambda path,body:(writes.append(body) or {'orderId':'o1'})
a._get=lambda path,params:{'list':[{'orderLinkId':'stable','orderStatus':'Filled','cumExecQty':'1','orderId':'o1'}]}
out=a.place_order('SOLUSDT','Buy',1,order_link_id='stable')
ok(out['fill_confirmed'] and writes[0]['orderLinkId']=='stable','exchange link and full-fill confirmation required')
def uncertain(path,body):raise bybit_api.BybitError('transport outcome uncertain')
a._post=uncertain
out=a.place_order('SOLUSDT','Buy',1,order_link_id='stable')
ok(out['fill_confirmed'] and out['recovered'],'uncertain submission reconciles existing linked fill, no replacement order')
a._get=lambda path,params:{'list':[{'orderLinkId':'stable','orderStatus':'Cancelled','cumExecQty':'0','orderId':'o1'}]}
try:a.place_order('SOLUSDT','Buy',1,order_link_id='stable')
except bybit_api.BybitError:ok(True,'cancelled or partial order cannot claim full fill')
else:raise AssertionError('unfilled order accepted')
# Directional rounding never crosses the intended BE floor.
for side,floor,price in [('long',100.125,100.7),('short',99.875,99.3)]:
 key='booobsas|SOL/'+side+'|floor';st=empty();st['mirrored'][key]={'symbol':'SOLUSDT','side':side,'qty':1,'entry_price':100,'leverage':10}
 st['retained_trailing']={key:{'symbol':'SOLUSDT','side':side,'qty':1,'fee_be':floor,'activation_threshold':floor*(1.004 if side=='long' else .996),'status':'pending','best_price':price,'current_sl':None,'price_target':None}}
 f=FakeClient();f.ticker_prices['SOLUSDT']=price;f.live_positions=[{'symbol':'SOLUSDT','side':'Buy' if side=='long' else 'Sell','size':'1','avgPrice':'100'}]
 listener.manage_retained_trailing_stops(f,st);sl=st['retained_trailing'][key]['current_sl']
 ok(sl>=floor if side=='long' else sl<=floor,side+' tick rounding preserves breakeven floor')
# Startup creates a running manager, not merely a function called every five minutes.
listener.save_state(empty());listener.BybitClient=lambda:FakeClient()
async def lifecycle():
 await listener.startup_risk_manager();await asyncio.sleep(.05)
 ok(listener.RISK_TASK is not None and not listener.RISK_TASK.done(),'startup runs independent stop-manager task')
 await listener.shutdown_risk_manager()
asyncio.run(lifecycle())
# Failed source close is retried after source disappears from the next snapshot.
class OnceFailure(FakeClient):
 def __init__(self):
  super().__init__();self.fail=True;self.ticker_prices['SOLUSDT']=99
  self.live_positions=[{'symbol':'SOLUSDT','side':'Buy','size':'1','avgPrice':'100'}]
 def set_sl_tp(self,*args,**kwargs):
  if self.fail:self.fail=False;raise bybit_api.BybitError('temporary stop error')
  return super().set_sl_tp(*args,**kwargs)
f=OnceFailure();listener.BybitClient=lambda:f
p=pos(ticker='SOL',side='long',sid='retry',ep=100,cp=99,lev=10)
k=listener.tkey('booobsas',p);st=empty();st['books']=make_book([p]);st['mirrored'][k]={'symbol':'SOLUSDT','side':'long','qty':1,'entry_price':100,'leverage':10}
listener.save_state(st)
payload=listener.WebhookPayload(source='test',fired_at=datetime.now(timezone.utc).isoformat(),books=make_book([]),hold_new=False)
first=asyncio.run(listener.involio_delta(payload,x_signature='testsecret'))
ok(first['errors']==1 and k in listener.load_state()['source_close_pending'],'failed source close remains queued in persistent state')
second=asyncio.run(listener.involio_delta(payload,x_signature='testsecret'))
ok(k in listener.load_state()['retained_trailing'],'source-close retry works without another source delta')
ok(not f.orders,'losing source close never sends a market close order')
# Status/log compatibility remains available to the cheap alert gate.
listener.save_state(empty());listener.BybitClient=lambda:FakeClient()
h=listener.health()
ok(h['code_version']==listener.LISTENER_VERSION and 'log_tail' in h and 'margin' in h,'legacy status and credit-saving alert fields remain available')
ok('lines' in listener.log_endpoint(),'account-only alert collector retains log endpoint')
# A changed source target cannot replay an uncertain earlier resize.
f=FakeClient(wallet=1000,avail=1000);f.ticker_prices['SOLUSDT']=100
f.get_linked_order=lambda symbol,link:{'orderStatus':'Filled','cumExecQty':'.2'}
p=pos(ticker='SOL',side='long',sid='resize',sq=1.3,ep=100,cp=100,lev=10)
k=listener.tkey('booobsas',p);st=empty()
r={'symbol':'SOLUSDT','side':'long','qty':1,'leverage':10,'entry_price':100,'last_applied_source_qty':1,
   'pending_resize':{'link':'earlier','target_source_qty':1.2,'prev_source_qty':1,'prev_owner_qty':1,'delta_qty':.2,'direction':'add'}}
st['mirrored'][k]=r
listener.sync_size(f,'booobsas',p,r,st)
ok(len(f.orders)==1 and abs(f.orders[0]['qty']-.1)<1e-9,'earlier confirmed resize is reconciled, only residual target is submitted')
ok(abs(r['qty']-1.3)<1e-9 and r['resize_generation']==2,'resized position and link generation remain consistent')
# Recover an actual partial initial fill and protect it without reopening.
f=FakeClient(wallet=1000,avail=1000);f.ticker_prices['SOLUSDT']=100
f.live_positions=[{'symbol':'SOLUSDT','side':'Buy','size':'.5','avgPrice':'100'}]
f.get_linked_order=lambda symbol,link:{'orderStatus':'PartiallyFilledCanceled','cumExecQty':'.5'}
p=pos(ticker='SOL',side='long',sid='park',sq=1,ep=100,cp=100,lev=10)
k=listener.tkey('booobsas',p);st=empty();st['books']=make_book([p]);st['parked_ambiguous']={k:{'order_link_id':'link','symbol':'SOLUSDT','position':p,'requested_qty':1,'leverage':10}}
listener.recover_parked_entries(f,st)
ok(st['mirrored'][k]['qty']==.5 and not f.orders,'uncertain partial entry recovery sends no duplicate order')
ok(st['mirrored'][k]['last_applied_source_qty']==.5 and st['mirrored'][k]['desired_source_qty']==1,'partial fill keeps full declared sizing intent queued')
ok(bool(f.stops) and not st['parked_ambiguous'],'confirmed partial fill has source protection applied')
# API exposes a partial fill as partial, never as the requested full quantity.
a=bybit_api.BybitClient(api_key='',api_secret='');a._post=lambda path,body:{'orderId':'partial'}
a._get=lambda path,params:{'list':[{'orderLinkId':'partial','orderStatus':'PartiallyFilledCanceled','cumExecQty':'.5','orderId':'partial'}]}
x=a.place_order('SOLUSDT','Buy',1,order_link_id='partial')
ok(x['partial_fill'] and x['filled_qty']==.5,'actual partial executed quantity is returned explicitly')

# A root-owned .env is intentionally unreadable by the service user; systemd supplies its environment.
import dotenv,importlib.util
saved_loader=dotenv.load_dotenv
def root_only(*args,**kwargs):raise PermissionError('root-only environment file')
dotenv.load_dotenv=root_only
try:
 spec=importlib.util.spec_from_file_location('listener_permission_test',listener.__file__)
 module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
 ok(module.LISTENER_VERSION==listener.LISTENER_VERSION,'service starts with protected root-only env supplied by systemd')
finally:dotenv.load_dotenv=saved_loader
print('ALL '+str(PASS)+' REGRESSION CHECKS PASSED')
