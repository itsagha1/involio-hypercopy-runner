"""Read-only Involio adapter. All credentials stay in the VPS environment."""
from datetime import datetime, timezone
import math, os, requests
PROFILE = 'booobsas'
PORTFOLIO_ID = '1c4f8dfd-3e4c-4378-b6fd-a0f694c10fd3'
BASE = 'https://api.involio.com'

class SourceDataError(RuntimeError):
    pass

def positive(value, name):
    try: value = float(value)
    except (TypeError, ValueError): raise SourceDataError('Invalid '+name)
    if not math.isfinite(value) or value <= 0:
        raise SourceDataError('Nonpositive '+name)
    return value

def normalize_source(meta, raw_positions, sims):
    portfolio = meta.get('portfolio') or {}
    if meta.get('success') is not True or portfolio.get('id') != PORTFOLIO_ID:
        raise SourceDataError('Wrong or incomplete source portfolio')
    if (portfolio.get('owner') or {}).get('username') != PROFILE:
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
        if (position.get('portfolio') or {}).get('id') != PORTFOLIO_ID:
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
            'source_portfolio_id':PORTFOLIO_ID,'source_profile':PROFILE,
            'remaining_sim':remaining,'complete':True,
            'snapshot_at':datetime.now(timezone.utc).isoformat(),
            'source_total_fees':sims.get('portfolioTotalFeeSim')}

def fetch_source_book():
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
    metadata=post('/v1_0/portfolios/get_portfolio_by_id',{'portfolioId':PORTFOLIO_ID})
    positions=[]
    for page in range(1,22):
        if page>20: raise SourceDataError('Source pagination exceeded safety bound')
        value=post('/v1_0/investments/get_investments',{'portfolioId':PORTFOLIO_ID,'isOpen':True,'params':{'page':page,'size':50}})
        batch=value.get('investmentsTicker')
        if not isinstance(batch,list): raise SourceDataError('Missing open-position page')
        positions.extend(batch)
        if len(batch)<50: break
    sims=post('/v1_0/investments/get_investments_sims',{'portfolioId':PORTFOLIO_ID})
    return normalize_source(metadata,positions,sims)
