"""Per-source mirror performance stats (owner 2026-10-20).
Reconstructs which trades OUR engines mirrored per source profile from the
persistent actions logs, then joins the close events with Bybit's
closed-position realized-PnL records, so the dashboard can show OUR stats
per source (total mirrored trades, wins/losses, realized P/L), not the
source's own Involio history. Works for both listener log formats:
  [2026-10-18 12:05:00 UTC] kings: CLOSED xk1ngss|W/long|<uuid> ...
  [2026-10-20T03:55:15+00:00] CLOSED booobsas|SOL/long|<uuid> ...
"""
import re
from datetime import datetime, timezone

_KEY_RE = re.compile(r'([A-Za-z0-9_,]+\|[^|]+/(?:long|short)\|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})')
_TS_RE = re.compile(r'\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})')

# coin -> Bybit symbol exceptions for logs that only carry the raw coin name
_SYMBOL_EXCEPTIONS = {
    'kPEPE': '1000PEPE', 'PEPE': '1000PEPE', 'SHIB': '1000SHIB',
    'FLOKI': '1000FLOKI', 'BONK': '1000BONK', 'LUNA': 'LUNA2',
}


def default_symbol_of(coin):
    coin = str(coin)
    if coin.endswith('USDT'):
        return coin
    return _SYMBOL_EXCEPTIONS.get(coin, coin) + 'USDT'


def _parse_ts(text):
    try:
        if 'T' in text:
            t = datetime.fromisoformat(text)
            return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
        return datetime.strptime(text, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _events(log_lines):
    opens, closes, seen_open, seen_close = [], [], set(), set()
    for line in log_lines:
        m = _KEY_RE.search(line)
        if not m:
            continue
        key = m.group(1)
        tm = _TS_RE.search(line)
        if not tm:
            continue
        ts = _parse_ts(tm.group(1))
        if ts is None:
            continue
        if 'OPENED' in line:
            if key not in seen_open:
                seen_open.add(key)
                opens.append((ts, key))
        elif ('CLOSED' in line or 'RETAINED-EXITED' in line) and key not in seen_close:
            seen_close.add(key)
            closes.append((ts, key))
    return opens, closes


def _blank():
    return {'trades': 0, 'wins': 0, 'losses': 0, 'realized_pnl': 0.0,
            'open_now': 0, 'closes_unmatched': 0}


def build(log_lines, closed_records, id_to_profile=None, current_open=None, symbol_of=None):
    """Returns {profiles: {name: stats}, cumulative_pnl, closes_unmatched_total}."""
    symbol_of = symbol_of or default_symbol_of
    current_open = current_open or {}
    opens, closes = _events(log_lines)

    def profile_of(key):
        parts = key.split('|')
        sid = parts[-1]
        if id_to_profile and sid in id_to_profile:
            return id_to_profile[sid]
        return parts[0].split(',')[0]

    used = set()
    matched = []
    unmatched_total = 0
    late_events = []
    for ts, key in closes:
        coin_side = key.split('|')[1]
        coin = coin_side.rsplit('/', 1)[0]
        side = 'long' if coin_side.endswith('/long') else 'short'
        sym = symbol_of(coin)
        # Bybit closed-pnl 'side' is the CLOSING order side: long close -> Sell
        want = 'Sell' if side == 'long' else 'Buy'
        best_i, best_d = None, None
        for i, r in enumerate(closed_records):
            if i in used or r.get('symbol') != sym or (r.get('side') or '') != want:
                continue
            try:
                rts = datetime.fromtimestamp(int(r.get('createdTime')) / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError, OSError, OverflowError):
                continue
            d = abs((rts - ts).total_seconds())
            if d <= 900 and (best_d is None or d < best_d):
                best_i, best_d = i, d
        pnl = None
        if best_i is not None:
            used.add(best_i)
            try:
                pnl = float(closed_records[best_i].get('closedPnl') or 0.0)
            except (TypeError, ValueError):
                pnl = None
        if pnl is None:
            late_events.append((ts, key))
        else:
            matched.append((key, pnl))

    # Second pass (owner 2026-10-20): closes logged long after the exchange
    # already flattened the position (already-flat deregister lines - the real
    # close was a stop, TP or liquidation hours/days earlier). The true record
    # is the LATEST unused record for that symbol+side closed BEFORE the line.
    for ts, key in late_events:
        coin_side = key.split('|')[1]
        coin = coin_side.rsplit('/', 1)[0]
        side = 'long' if coin_side.endswith('/long') else 'short'
        sym = symbol_of(coin)
        want = 'Sell' if side == 'long' else 'Buy'
        best_i, best_ct = None, None
        for i, r in enumerate(closed_records):
            if i in used or r.get('symbol') != sym or (r.get('side') or '') != want:
                continue
            try:
                rts = datetime.fromtimestamp(int(r.get('createdTime')) / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError, OSError, OverflowError):
                continue
            if (rts - ts).total_seconds() > 900:
                continue
            if best_ct is None or rts > best_ct:
                best_i, best_ct = i, rts
        pnl = None
        if best_i is not None:
            used.add(best_i)
            try:
                pnl = float(closed_records[best_i].get('closedPnl') or 0.0)
            except (TypeError, ValueError):
                pnl = None
        if pnl is None:
            unmatched_total += 1
        matched.append((key, pnl))

    # Owner 2026-10-20: count only trades with REAL exchange history. A trade
    # enters the stats when its close matched a realized-PnL record or it is
    # still open now. Log events with no exchange trace (dry-run era,
    # bookkeeping deregisters) are excluded from totals but surfaced via
    # closes_unmatched. current_open maps open mirror keys -> profile name.
    matched_keys = {}
    for key, pnl in matched:
        if pnl is not None:
            matched_keys[key] = pnl
    trade_keys = set(k for k in matched_keys)
    for key in current_open:
        trade_keys.add(key)

    profiles = {}
    for key in trade_keys:
        prof = current_open.get(key) or profile_of(key)
        p = profiles.setdefault(prof, _blank())
        p['trades'] += 1
        pnl = matched_keys.get(key)
        if key not in current_open:
            if pnl is None:
                p['closes_unmatched'] += 1
            elif pnl >= 0:
                p['wins'] += 1
                p['realized_pnl'] += pnl
            else:
                p['losses'] += 1
                p['realized_pnl'] += pnl
    for key, prof in current_open.items():
        profiles.setdefault(prof, _blank())['open_now'] += 1

    cumulative = 0.0
    for r in closed_records:
        try:
            cumulative += float(r.get('closedPnl') or 0.0)
        except (TypeError, ValueError):
            pass
    for p in profiles.values():
        p['realized_pnl'] = round(p['realized_pnl'], 4)
    return {'profiles': profiles, 'cumulative_pnl': round(cumulative, 4),
            'closes_unmatched_total': unmatched_total}
