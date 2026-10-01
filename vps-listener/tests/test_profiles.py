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
