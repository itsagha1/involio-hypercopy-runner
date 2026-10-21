"""Read-only Involio adapter. All credentials stay in the VPS environment."""
from datetime import datetime, timezone
import math, os, requests
PROFILE = 'booobsas'
PORTFOLIO_ID = '1c4f8dfd-3e4c-4378-b6fd-a0f694c10fd3'
PROFILES={'booobsas':PORTFOLIO_ID,'akira':'72813ea9-35db-4ffe-b0b1-2d71cbbac837','oozypath':'2f5cb886-dfa0-457a-976e-4bc9c13ce35e','_k2_':'b34527e7-44e3-4e33-94a0-c2b7d637c6c6'}
BASE = 'https://api.involio.com'

class SourceDataError(RuntimeError):
    pass

def positive(value, name):
    try: value = float(value)
    except (TypeError, ValueError): raise SourceDataError('Invalid '+name)
    if not math.isfinite(value) or value <= 0:
        raise SourceDataError('Nonpositive '+name)
    return value

def normalize_source(meta, raw_positions, sims, profile=PROFILE, portfolio_id=None):
    # owner 2026-10-16: profile may be comma-separated (multi-owner subaccount,
    # e.g. bluespire,b4451). Every name must be registered; the portfolio's
    # owner username must be one of them.
    allowed = {x.strip() for x in str(profile).split(",") if x.strip()}
    if not allowed:
        raise SourceDataError("Unauthorized source profile")
    for name in allowed:
        if name not in PROFILES:
            raise SourceDataError("Unauthorized source profile")
    portfolio_id = portfolio_id or PROFILES[profile]
    portfolio = meta.get('portfolio') or {}
    if meta.get('success') is not True or portfolio.get('id') != portfolio_id:
        raise SourceDataError('Wrong or incomplete source portfolio')
    if (portfolio.get('owner') or {}).get('username') not in allowed:
        raise SourceDataError('Wrong source owner')
    if sims.get('success') is not True or not isinstance(sims.get('investments'), list):
        raise SourceDataError('Incomplete source sim data')
    expected = int(portfolio.get('openPositions', -1))
    if expected != len(raw_positions) or len(sims['investments']) != len(raw_positions):
        raise SourceDataError('Source snapshot counts disagree; not safe to infer closures')
    sim_by_id = {str(x.get('baseId')): x for x in sims['investments']}
    if len(sim_by_id) != len(sims['investments']):
        raise SourceDataError('Duplicate source sim identities')
    positions=[];keys=set();ids=set()
    for position in raw_positions:
        identity=str(position.get('baseId') or '')
        if not identity or identity in ids or identity not in sim_by_id:
            raise SourceDataError('Missing or duplicate source trade identity')
        ids.add(identity)
        sim=sim_by_id[identity]
        if position.get('isOpen') is not True:
            raise SourceDataError('Closed position present in open snapshot')
        margin=positive(position.get('entrySim'), 'source margin')
        other=positive(sim.get('entrySim'), 'source sim margin')
        if abs(margin-other)>max(1e-7,margin*1e-7):
            raise SourceDataError('Source changed while snapshot was fetched')
        leverage=positive(position.get('leverage'), 'leverage')
        entry=positive(position.get('entryPrice'), 'entry price')
        current=positive(position.get('currentPrice'), 'current price')
        pct=positive(position.get('entrySize'), 'declared allocation percentage')
        if pct>100: raise SourceDataError('Invalid declared allocation percentage')
        if not isinstance(position.get('directionLong'), bool):
            raise SourceDataError('Missing source direction')
        if (position.get('portfolio') or {}).get('id') != portfolio_id:
            raise SourceDataError('Position belongs to a different source portfolio')
        ticker=str(position.get('ticker') or '')
        if not ticker: raise SourceDataError('Missing source ticker')
        side='long' if position.get('directionLong') is True else 'short'
        key=ticker+'/'+side
        if key in keys: raise SourceDataError('Duplicate source symbol/side; cannot safely aggregate')
        keys.add(key)
        positions.append({'source_id':identity,'ticker':ticker,'side':side,
                          'source_margin':margin,'source_qty':margin*leverage/entry,
                          'source_allocation_pct':pct,'allocation_verified':True,
                          'entry_sim':margin,'last_sim':sim.get('lastSim'),
                          'entry_price':entry,'current_price':current,'leverage':leverage,
                          'stop_loss':position.get('stopLoss'),
                          'price_target':position.get('priceTarget'),
                          'created_at':position.get('createdAt'),'updated_at':position.get('updatedAt')})
    remaining=float(sims.get('portfolioRemainingSim'))
    if not math.isfinite(remaining) or remaining<0: raise SourceDataError('Invalid source cash balance')
    capital=remaining+sum(p['source_margin'] for p in positions)
    positive(capital,'source allocated-capital balance')
    return {'positions':positions,'source_equity':capital,'equity_verified':True,
            'source_equity_basis':'gross_sim_allocatable_capital_not_fee_net_mark_equity',
            'allocation_basis':'declared_entrySize_percent',
            'source_portfolio_id':portfolio_id,'source_profile':profile,
            'remaining_sim':remaining,'complete':True,
            'snapshot_at':datetime.now(timezone.utc).isoformat(),
            'source_total_fees':sims.get('portfolioTotalFeeSim')}

def fetch_dex_book(session, profile=PROFILE, portfolio_id=None, portfolio=None):
    """Owner 2026-10-19: LIVE portfolios (type='live', e.g. billionaireboysclub)
    serve their books from the /dex/ endpoint family, NOT the v1_0 investments
    API. Normalized to the same shape as the sim book so copy_listener needs no
    changes. sizing: size.pct is the notional allocation % of equity, size.cash
    is the notional; margin = cash / leverage; roi from net pnl pct."""
    allowed = {x.strip() for x in str(profile).split(',') if x.strip()}
    if not allowed: raise SourceDataError('Unauthorized source profile')
    for name in allowed:
        if name not in PROFILES: raise SourceDataError('Unauthorized source profile')
    if portfolio is None or portfolio.get('id') != portfolio_id:
        raise SourceDataError('Wrong or incomplete source portfolio')
    if (portfolio.get('owner') or {}).get('username') not in allowed:
        raise SourceDataError('Wrong source owner')
    def get(path, params=None):
        response=session.get(BASE+path, params=params or {}, timeout=25)
        response.raise_for_status()
        value=response.json()
        if value.get('success') is not True or value.get('error'):
            raise SourceDataError('DEX API failed for '+path)
        return value.get('data') or {}
    data=get('/dex/portfolio/position/'+str(portfolio_id)+'/list', {'isOpen':'true','limit':'100'})
    raw=data.get('positions')
    if not isinstance(raw, list): raise SourceDataError('Missing DEX open-position list')
    overview=get('/dex/portfolio/live/'+str(portfolio_id)+'/overview')
    equity=positive((overview.get('equity') or {}).get('value'), 'DEX equity')
    positions=[];keys=set();ids=set()
    for p in raw:
        identity=str(p.get('id') or '')
        ticker=str(p.get('assetSymbol') or '')
        side=str(p.get('side') or '')
        if not identity or identity in ids or not ticker or side not in ('long','short'):
            raise SourceDataError('Missing/invalid DEX trade identity')
        ids.add(identity)
        key=ticker+'/'+side
        if key in keys: raise SourceDataError('Duplicate source symbol/side; cannot safely aggregate')
        keys.add(key)
        leverage=positive(p.get('leverage'), 'leverage')
        entry=positive(p.get('entryPx'), 'entry price')
        current=positive(p.get('markPx'), 'mark price')
        cash=positive((p.get('size') or {}).get('cash'), 'DEX position cash')
        pct=positive((p.get('size') or {}).get('pct'), 'declared allocation percentage')
        if pct>100: raise SourceDataError('Invalid declared allocation percentage')
        try: net_pct=float(((p.get('pnl') or {}).get('net') or {}).get('pct') or 0)
        except (TypeError, ValueError): raise SourceDataError('Invalid DEX net pnl')
        margin=cash/leverage
        positions.append({'source_id':identity,'ticker':ticker,'side':side,
                          'source_margin':margin,'source_qty':cash/entry,
                          'source_allocation_pct':pct,'allocation_verified':True,
                          'entry_sim':margin,'last_sim':margin*(1.0+net_pct/100.0),
                          'entry_price':entry,'current_price':current,'leverage':leverage,
                          'stop_loss':p.get('stopLossPx'),
                          'price_target':p.get('takeProfitPx'),
                          'created_at':p.get('openedAt'),'updated_at':p.get('openedAt')})
    remaining=equity-sum(pp['source_margin'] for pp in positions)
    if not math.isfinite(remaining) or remaining<0: raise SourceDataError('Invalid DEX source cash balance')
    return {'positions':positions,'source_equity':equity,'equity_verified':True,
            'source_equity_basis':'dex_live_overview_equity',
            'allocation_basis':'declared_size_pct',
            'source_portfolio_id':portfolio_id,'source_profile':profile,
            'remaining_sim':remaining,'complete':True,
            'snapshot_at':datetime.now(timezone.utc).isoformat()}


def fetch_source_book(profile=PROFILE, portfolio_id=None):

    if portfolio_id is None:
        if profile not in PROFILES:raise SourceDataError("Unauthorized source profile")
        portfolio_id=PROFILES[profile]
    token=os.environ.get('INVOLIO_REFRESH_TOKEN','')
    if not token: raise SourceDataError('Involio source credential missing on VPS')
    session=requests.Session()
    session.headers.update({'Accept':'application/json','Content-Type':'application/json','User-Agent':'Mozilla/5.0'})
    response=session.get(BASE+'/v1_0/auth/refresh_token',headers={'Authorization':'Bearer '+token},timeout=25)
    response.raise_for_status()
    access=response.json().get('accessToken')
    if not access: raise SourceDataError('Involio refresh did not return access token')
    session.headers['Authorization']='Bearer '+access
    def post(path,data):
        response=session.post(BASE+path,json=data,timeout=25)
        response.raise_for_status();value=response.json()
        if value.get('success') is not True or value.get('error'):
            raise SourceDataError('Source API failed for '+path)
        return value
    metadata=post('/v1_0/portfolios/get_portfolio_by_id',{'portfolioId':portfolio_id})
    if (metadata.get('portfolio') or {}).get('type') == 'live':
        return fetch_dex_book(session, profile, portfolio_id, metadata.get('portfolio'))
    positions=[]
    for page in range(1,22):
        if page>20: raise SourceDataError('Source pagination exceeded safety bound')
        value=post('/v1_0/investments/get_investments',{'portfolioId':portfolio_id,'isOpen':True,'params':{'page':page,'size':50}})
        batch=value.get('investmentsTicker')
        if not isinstance(batch,list): raise SourceDataError('Missing open-position page')
        positions.extend(batch)
        if len(batch)<50: break
    sims=post('/v1_0/investments/get_investments_sims',{'portfolioId':portfolio_id})
    return normalize_source(metadata,positions,sims,profile,portfolio_id)


def fetch_closed_base_ids(profile=PROFILE, portfolio_id=None, pages=2):
    """Map of recently-closed Involio positions keyed by their ORIGINAL open
    investment id (owner 2026-10-18). Closed records carry the open id in baseId.
    Used by copy_listener to distinguish a normal source close (must be followed
    at market, 100% mirror) from a genuinely vanished position (retention case)."""
    if portfolio_id is None:
        if profile not in PROFILES:raise SourceDataError("Unauthorized source profile")
        portfolio_id=PROFILES[profile]
    token=os.environ.get('INVOLIO_REFRESH_TOKEN','')
    if not token: raise SourceDataError('Involio source credential missing on VPS')
    session=requests.Session()
    session.headers.update({'Accept':'application/json','Content-Type':'application/json','User-Agent':'Mozilla/5.0'})
    response=session.get(BASE+'/v1_0/auth/refresh_token',headers={'Authorization':'Bearer '+token},timeout=25)
    response.raise_for_status()
    access=response.json().get('accessToken')
    if not access: raise SourceDataError('Involio refresh did not return access token')
    session.headers['Authorization']='Bearer '+access
    meta=session.post(BASE+'/v1_0/portfolios/get_portfolio_by_id',json={'portfolioId':portfolio_id},timeout=25)
    meta.raise_for_status();meta=meta.json()
    if meta.get('success') is not True or (meta.get('portfolio') or {}).get('id') != portfolio_id:
        raise SourceDataError('Source API failed for portfolio metadata')
    closed={}
    if (meta.get('portfolio') or {}).get('type') == 'live':
        response=session.get(BASE+'/dex/portfolio/position/'+str(portfolio_id)+'/list',params={'isOpen':'false','limit':'100'},timeout=25)
        response.raise_for_status();value=response.json()
        if value.get('success') is not True or value.get('error'):
            raise SourceDataError('DEX closed-list failed')
        for x in (value.get('data') or {}).get('positions') or []:
            closed[str(x.get('id'))]={'ticker':x.get('assetSymbol'),'closedAt':x.get('closedAt'),'reason':x.get('closeReason')}
        return closed
    for page in range(1,max(1,int(pages))+1):
        response=session.post(BASE+'/v1_0/investments/get_investments',json={'portfolioId':portfolio_id,'isOpen':False,'params':{'page':page,'size':50}},timeout=25)
        response.raise_for_status();value=response.json()
        if value.get('success') is not True or value.get('error'):
            raise SourceDataError('Source API failed for closed investments')
        batch=value.get('investmentsTicker')
        if not isinstance(batch,list): raise SourceDataError('Missing closed-position page')
        for x in batch:
            bid=x.get('baseId')
            if bid:
                closed[bid]={'ticker':x.get('ticker'),'closedAt':x.get('closedAt'),'reason':x.get('reasonClosed')}
        if len(batch)<50: break
    return closed


def fetch_source_stats(profile=PROFILE, portfolio_id=None):
    """Owner 2026-10-19: per-source performance stats for the dashboard:
    trades within the last 24h (closed in window + opened in window and still
    open), open count, total trades, wins/losses and total P/L. paper/sim
    portfolios use portfolio metadata (closedPositions, winRate, plSnapshot)
    plus closed/open book scans for the 24h window; live (DEX) portfolios use
    /dex/portfolio/live/<pid>/overview (allTimePnl, closedCount, accuracyPct,
    pnl24h) plus the DEX open/closed lists."""
    from datetime import timedelta
    if portfolio_id is None:
        if profile not in PROFILES: raise SourceDataError('Unauthorized source profile')
        portfolio_id = PROFILES[profile]
    token=os.environ.get('INVOLIO_REFRESH_TOKEN','')
    if not token: raise SourceDataError('Involio source credential missing on VPS')
    session=requests.Session()
    session.headers.update({'Accept':'application/json','Content-Type':'application/json','User-Agent':'Mozilla/5.0'})
    response=session.get(BASE+'/v1_0/auth/refresh_token',headers={'Authorization':'Bearer '+token},timeout=25)
    response.raise_for_status()
    access=response.json().get('accessToken')
    if not access: raise SourceDataError('Involio refresh did not return access token')
    session.headers['Authorization']='Bearer '+access
    def post(path,data):
        response=session.post(BASE+path,json=data,timeout=25)
        response.raise_for_status();value=response.json()
        if value.get('success') is not True or value.get('error'):
            raise SourceDataError('Source API failed for '+path)
        return value
    meta=post('/v1_0/portfolios/get_portfolio_by_id',{'portfolioId':portfolio_id})
    portfolio=meta.get('portfolio') or {}
    if portfolio.get('id') != portfolio_id:
        raise SourceDataError('Wrong or incomplete source portfolio')
    now=datetime.now(timezone.utc)
    def recent(iso):
        if not iso: return False
        try: t=datetime.fromisoformat(str(iso).replace('Z','+00:00'))
        except (TypeError, ValueError): return False
        if t.tzinfo is None: t=t.replace(tzinfo=timezone.utc)
        return (now-t) <= timedelta(hours=24)
    if portfolio.get('type')=='live':
        def get(path,params=None):
            response=session.get(BASE+path,params=params or {},timeout=25)
            response.raise_for_status();value=response.json()
            if value.get('success') is not True or value.get('error'):
                raise SourceDataError('DEX API failed for '+path)
            return value.get('data') or {}
        overview=get('/dex/portfolio/live/'+str(portfolio_id)+'/overview')
        cl=get('/dex/portfolio/position/'+str(portfolio_id)+'/list',{'isOpen':'false','limit':'100'})
        op=get('/dex/portfolio/position/'+str(portfolio_id)+'/list',{'isOpen':'true','limit':'100'})
        closed_recent=sum(1 for x in (cl.get('positions') or []) if recent(x.get('closedAt')))
        opened_recent=sum(1 for x in (op.get('positions') or []) if recent(x.get('openedAt')))
        closed_total=int(float(overview.get('closedCount') or 0))
        acc=float(overview.get('accuracyPct') or 0)
        wins=int(round(acc/100.0*closed_total)) if closed_total else 0
        atp=overview.get('allTimePnl') or {}
        p24=overview.get('pnl24h') or {}
        open_now=int((overview.get('openTrades') or {}).get('count') or 0)
        return {'profile':profile,'portfolio_title':portfolio.get('title'),'type':'live','unit':'cash',
                'open_now':open_now,'trades_24h':closed_recent+opened_recent,
                'closed_24h':closed_recent,'opened_24h':opened_recent,
                'total_trades':closed_total+open_now,'wins':wins,'losses':max(0,closed_total-wins),
                'total_pnl':float(atp.get('cash') or 0),'total_pnl_pct':float(atp.get('pct') or 0),
                'pnl_24h':float(p24.get('cash') or 0),
                'equity':float((overview.get('equity') or {}).get('value') or 0)}
    closed=[]
    for page in (1,2):
        value=post('/v1_0/investments/get_investments',{'portfolioId':portfolio_id,'isOpen':False,'params':{'page':page,'size':50}})
        batch=value.get('investmentsTicker')
        if not isinstance(batch,list): raise SourceDataError('Missing closed-position page')
        closed.extend(batch)
        if len(batch)<50: break
    opened=post('/v1_0/investments/get_investments',{'portfolioId':portfolio_id,'isOpen':True,'params':{'page':1,'size':50}}).get('investmentsTicker') or []
    closed_total=int(float(portfolio.get('closedPositions') or 0))
    open_now=int(float(portfolio.get('openPositions') or 0))
    closed_recent=[x for x in closed if recent(x.get('closedAt'))]
    opened_recent=sum(1 for x in opened if recent(x.get('createdAt')))
    win_rate=float(portfolio.get('winRate') or 0)
    wins=int(round(win_rate/100.0*closed_total)) if closed_total else 0
    pnl_24h=0.0
    for x in closed_recent:
        try: pnl_24h+=float(x.get('lastSim') or 0)-float(x.get('entrySim') or 0)
        except (TypeError, ValueError): pass
    return {'profile':profile,'portfolio_title':portfolio.get('title'),'type':'paper','unit':'sim',
            'open_now':open_now,'trades_24h':len(closed_recent)+opened_recent,
            'closed_24h':len(closed_recent),'opened_24h':opened_recent,
            'total_trades':closed_total+open_now,'wins':wins,'losses':max(0,closed_total-wins),
            'total_pnl':float(portfolio.get('plSnapshot') or 0),'total_pnl_pct':None,
            'pnl_24h':pnl_24h,'equity':None}
