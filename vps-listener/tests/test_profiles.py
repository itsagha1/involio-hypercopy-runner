"""Offline multi-profile isolation and baseline regressions. No network or orders."""
from __future__ import annotations
import os,sys,tempfile,asyncio,copy
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
from test_rules import FakeClient,pos,make_book
import listener,source_api
listener.STATE_FILE=os.path.join(tempfile.mkdtemp(),'state.json');listener.LOG_FILE=listener.STATE_FILE+'.log'
PASS=0
def ok(value,label):
 global PASS
 assert value,label
 PASS+=1;print('  ok - '+label)
def books(p):return make_book(p)['booobsas']
a=pos(ticker='ZEC',side='long',sid='ak-old',sq=1,lev=8)
b=pos(ticker='SOL',side='long',sid='boo-new',sq=1,lev=10)
dual={'booobsas':books([b]),'akira':books([a])}
ok(listener.validate_booobsas_book(dual)[0],'both registered profiles accepted')
ok(not listener.validate_booobsas_book({**dual,'limpan96':books([])})[0],'unrequested profiles rejected')
d=listener.compute_deltas({'booobsas':books([]),'akira':books([])},dual)
ok({x['trader'] for x in d}=={'akira','booobsas'},'deltas attributed to the correct profile')
ok(listener.tkey('akira',a).startswith('akira|'),'Akira ownership key is distinct')
old_book={'booobsas':books([b]),'akira':books([a])}
d=listener.compute_deltas(old_book,{'booobsas':books([b])})
ok(not any(x['trader']=='akira' for x in d),'omitted profile never infers source closes')
st={'mirrored':{listener.tkey('akira',a):{'symbol':'ZECUSDT','side':'long','qty':1,'leverage':8,'entry_price':120}},'manual':[]}
ok(listener.migrate_legacy_mirrors(st)==[],'Akira owned mirrors are not migrated to manual')
# Adding a profile while hold is on proves baseline preservation without executing any orders.
f=FakeClient();listener.BybitClient=lambda:f
st={'books':{'booobsas':books([b])},'baseline':['old-boo-baseline'],'mirrored':{},'manual':[],
 'manual_adopted':True,'fresh_start_at':books([])['snapshot_at'],'hold_new':True,'cutover_armed':True}
listener.save_state(st)
r=asyncio.run(listener.involio_delta(listener.WebhookPayload(source='test',books=dual,hold_new=True),x_signature='testsecret'))
st=listener.load_state()
ok(r['ok'] and 'akira|ak-old' in st['baseline'],'first Akira snapshot is baselined')
ok('old-boo-baseline' in st['baseline'] and 'boo-new' not in st['baseline'],'existing booobsas baseline preserved, never rebaselined')
ok(not f.orders,'profile addition sends no backfill order')
r=asyncio.run(listener.involio_delta(listener.WebhookPayload(source='test',books={'booobsas':books([b])},hold_new=True),x_signature='testsecret'))
ok('akira' in listener.load_state()['books'],'partial book delivery preserves Akira tracking')
a2=pos(ticker='ETH',side='long',sid='ak-new',sq=2,lev=10)
d=listener.compute_deltas(listener.load_state()['books'],{'booobsas':books([b]),'akira':books([a,a2])})
ok(any(x['trader']=='akira' and x['type']=='new_entry' and x['position']['source_id']=='ak-new' for x in d),'future Akira trade eligible after baseline')
# Pure source identity checks for Crypto, not some other Akira portfolio.
meta={'success':True,'portfolio':{'id':source_api.PROFILES['akira'],'owner':{'username':'akira'},'openPositions':1}}
raw={'baseId':'c1','portfolio':{'id':source_api.PROFILES['akira']},'isOpen':True,'entrySim':100,'entrySize':1,'leverage':8,'entryPrice':100,'currentPrice':100,'directionLong':True,'ticker':'ZEC'}
sims={'success':True,'investments':[{'baseId':'c1','entrySim':100,'lastSim':100}],'portfolioRemainingSim':9900}
book=source_api.normalize_source(meta,[raw],sims,'akira')
ok(book['source_profile']=='akira' and book['positions'][0]['source_allocation_pct']==1,'Crypto declaration and profile identity verified')
wrong=copy.deepcopy(meta);wrong['portfolio']['id']='other-portfolio'
try:source_api.normalize_source(wrong,[raw],sims,'akira')
except source_api.SourceDataError:ok(True,'different Akira portfolio fails closed')
else:raise AssertionError('wrong portfolio accepted')
try:source_api.normalize_source(meta,[raw],sims,'limpan96')
except source_api.SourceDataError:ok(True,'adapter cannot fetch an unrequested profile')
else:raise AssertionError('unrequested profile accepted')
from bybit_api import coin_to_symbol, symbol_to_coin
ok(coin_to_symbol('PUMP')=='PUMPFUNUSDT','PUMP source ticker maps to the live Bybit PUMPFUN contract')
ok(symbol_to_coin('PUMPFUNUSDT')=='PUMP','PUMPFUN contract maps back to source ticker PUMP')
# Synthetic retry test: after mapping or transient failures, an already-seen open source entry is still eligible.
st={'books':{'akira':books([a])},'baseline':['akira|ac20167c-ad9c-4f2a-8add-4fd206dbf'],'mirrored':{},'manual':[],
 'manual_adopted':True,'fresh_start_at':books([])['snapshot_at'],'hold_new':False,'cutover_armed':True,'entry_blocks':{}}
listener.save_state(st); f=FakeClient(); f.ticker_prices['PUMPFUNUSDT']=0.00565; listener.BybitClient=lambda:f
fresh=pos(ticker='PUMP',side='short',sid='pump-fresh',sq=1000,lev=7,ep=0.00565,cp=0.00565)
asyncio.run(listener.involio_delta(listener.WebhookPayload(source='test',books={'akira':books([a,fresh])},hold_new=False),x_signature='testsecret'))
ok(any(o.get('symbol')=='PUMPFUNUSDT' for o in f.orders),'current unmirrored Akira PUMP is retried against live PUMPFUN contract')
import source_api as sa
orig=sa.fetch_source_book
calls={'n':0}
fake_book=dict(books([a]))
def flaky(profile=sa.PROFILE):
    calls['n']+=1
    if calls['n']<3:raise sa.SourceDataError('Source snapshot counts disagree; not safe to infer closures')
    return fake_book
sa.fetch_source_book=flaky
real_sleep=asyncio.sleep
slept=[]
async def fake_sleep(t):slept.append(t)
asyncio.sleep=fake_sleep
resp=asyncio.run(listener.source_snapshot(profile='akira',x_signature='testsecret'))
asyncio.sleep=real_sleep;sa.fetch_source_book=orig
ok(resp.get('ok') is True and calls['n']==3,'transient source inconsistency retried until a consistent snapshot')
ok([t for t in slept if t>0]==[2,4],'retry pauses briefly between attempts')
calls['n']=0
def hopeless(profile=sa.PROFILE):
    calls['n']+=1;raise sa.SourceDataError('credential missing')
sa.fetch_source_book=hopeless;asyncio.sleep=fake_sleep
try:asyncio.run(listener.source_snapshot(profile='akira',x_signature='testsecret'))
except Exception as ex:ok('503' in str(ex),'persistent source failure still fails closed with no book forwarded')
else:raise AssertionError('persistent failure was served')
finally:asyncio.sleep=real_sleep;sa.fetch_source_book=orig
print('ALL '+str(PASS)+' PROFILE CHECKS PASSED')

# --- owner 2026-10-08: third authorized profile oozypath (portfolio 2f5cb886..., title OOZYPATH) ---
oz1=pos(ticker='BTC',side='long',sid='oz-btc',sq=0.01,lev=30,ep=83559.8,cp=82351)
oz2=pos(ticker='ASTER',side='long',sid='oz-aster',sq=100,lev=4,ep=0.714,cp=0.706)
triple={'booobsas':books([b]),'akira':books([a]),'oozypath':books([oz1,oz2])}
st={'books':{'booobsas':books([b]),'akira':books([a])},'baseline':['old-boo-baseline'],'mirrored':{},'manual':[],
 'manual_adopted':True,'fresh_start_at':books([])['snapshot_at'],'hold_new':True,'cutover_armed':True}
listener.save_state(st); f=FakeClient(); listener.BybitClient=lambda:f
r=asyncio.run(listener.involio_delta(listener.WebhookPayload(source='test',books=triple,hold_new=True),x_signature='testsecret'))
st=listener.load_state()
ok(r['ok'] and 'oozypath|oz-btc' in st['baseline'] and 'oozypath|oz-aster' in st['baseline'],'first OOZYPATH snapshot baselines both existing positions')
ok(not f.orders,'OOZYPATH profile addition sends no backfill order')
ok('old-boo-baseline' in st['baseline'],'existing baselines preserved when adding OOZYPATH')
d=listener.compute_deltas(listener.load_state()['books'],{'booobsas':books([b]),'akira':books([a]),'oozypath':books([oz1,oz2,pos(ticker='XRP',side='short',sid='oz-new',sq=50,lev=10)])})
ok(any(x['trader']=='oozypath' and x['type']=='new_entry' and x['position']['source_id']=='oz-new' for x in d),'future OOZYPATH trade eligible after baseline')
ok(not any(x['trader']=='oozypath' and x['position']['source_id']=='oz-btc' for x in d),'baselined OOZYPATH positions never mirror')
# Source identity: the OOZYPATH portfolio id itself, not another portfolio of the same user.
meta_oz={'success':True,'portfolio':{'id':source_api.PROFILES['oozypath'],'owner':{'username':'oozypath'},'openPositions':1},'source_portfolio_id':source_api.PROFILES['oozypath']}
raw_oz={'baseId':'oz1','portfolio':{'id':source_api.PROFILES['oozypath']},'isOpen':True,'entrySim':100,'entrySize':1,'leverage':30,'entryPrice':83559.8,'currentPrice':82351,'directionLong':True,'ticker':'BTC'}
sims_oz={'success':True,'investments':[{'baseId':'oz1','entrySim':100,'lastSim':100}],'portfolioRemainingSim':389.4}
book_oz=source_api.normalize_source(meta_oz,[raw_oz],sims_oz,'oozypath')
ok(book_oz['source_profile']=='oozypath' and book_oz['positions'][0]['leverage']==30,'OOZYPATH declaration and profile identity verified')
wrong_oz=copy.deepcopy(meta_oz);wrong_oz['portfolio']['id']='other-portfolio'
try:source_api.normalize_source(wrong_oz,[raw_oz],sims_oz,'oozypath')
except source_api.SourceDataError:ok(True,'different OOZYPATH portfolio fails closed')
else:raise AssertionError('wrong oozypath portfolio accepted')
print('ALL '+str(PASS)+' PROFILE CHECKS PASSED (v3.7.0 oozypath)')

# --- v3.7.1: legacy dict in baseline must never crash a new profile baseline ---
legacy={'mirrored':{},'manual':[],'manual_adopted':True,'fresh_start_at':books([])['snapshot_at'],
 'hold_new':True,'cutover_armed':True,'hold_new':True,
 'baseline':[{'ticker':'ONDO','side':'long','source_id':'legacy-dict'},'booobsas|kept-str']}
listener.save_state(legacy)
r=asyncio.run(listener.involio_delta(listener.WebhookPayload(source='test',books={'oozypath':books([oz1])},hold_new=True),x_signature='testsecret'))
st=listener.load_state()
ok(r['ok'] and 'oozypath|oz-btc' in st['baseline'],'new profile baselines despite legacy dict in state')
ok(not any(isinstance(x,dict) for x in st['baseline']),'legacy dict entry dropped from baseline')
ok('booobsas|kept-str' in st['baseline'],'string baseline entries preserved through sanitization')
print('ALL '+str(PASS)+' PROFILE CHECKS PASSED (v3.7.1 sanitize)')

# --- v3.7.2: float-noise source-qty delta snaps and returns None (no exchange call) ---
noise_state={'mirrored':{'t|FIL/short|x':{'symbol':'FILUSDT','side':'short','qty':5.1,
  'last_applied_source_qty':5.1+5.66e-16,'desired_source_qty':5.1}},'manual':[],'manual_adopted':True}
listener.save_state(noise_state)
class _NoCall:
    configured=True
    def __getattr__(self,n): raise AssertionError('client must not be touched for noise delta')
a=listener.sync_size(_NoCall(),'t',{'ticker':'FIL','side':'short','source_id':'x','source_qty':5.1},
                     noise_state['mirrored']['t|FIL/short|x'],noise_state)
ok(a is None,'float-noise delta returns None (no log, no exchange call)')
ok(abs(noise_state['mirrored']['t|FIL/short|x']['last_applied_source_qty']-5.1)<1e-15,'noise snapped: last_applied equals source qty')

# --- v3.7.2: benign ratchet race (existing stop) logs LOG not ERROR; no stop stays ERROR ---
class _RaceClient:
    configured=True
    def get_ticker(self,s): return {'lastPrice':100.0}
    def get_instrument(self,s): return {'tickSize':'0.01','minQty':0.1,'qtyStep':0.1}
    def position(self,s): return {}
    def get_positions(self):
        return [{'symbol':'FILUSDT','side':'Sell','size':5.1,'avgPrice':2.0}]
    def get_linked_order(self,s,l): return None
race_state={'mirrored':{'t2|FIL/short|y':{'symbol':'FILUSDT','side':'short','qty':5.1}},
 'retained_trailing':{'t2|FIL/short|y':{'key':'t2|FIL/short|y','symbol':'FILUSDT','side':'short','qty':5.1,
  'owner_entry_price':3.0,'fee_be':3.0,'activation_threshold':2.98,'status':'active',
  'best_price':2.95,'current_sl':2.99,'price_target':None}},'manual':[],'manual_adopted':True}
listener.save_state(race_state)
logs=listener.manage_retained_trailing_stops(_RaceClient(),race_state)
ok(any(l.startswith('LOG') and 'still protects' in l for l in logs),'race with existing stop -> benign LOG')
ok(not any(l.startswith('ERROR') for l in logs),'race with existing stop -> no ERROR alert')
race_state['retained_trailing']['t2|FIL/short|y']['current_sl']=None
listener.save_state(race_state)
logs=listener.manage_retained_trailing_stops(_RaceClient(),race_state)
ok(any(l.startswith('ERROR') and 'no existing stop' in l for l in logs),'race without stop -> still ERROR alert')
print('ALL '+str(PASS)+' PROFILE CHECKS PASSED (v3.7.2 resilience)')
