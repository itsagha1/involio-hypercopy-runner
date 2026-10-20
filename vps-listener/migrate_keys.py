"""One-time state-key migration (owner 2026-10-20).

State keys used to embed the ACCOUNT-LEVEL profile string (COPY_PROFILE, which
is comma-joined for multi-profile subaccounts), so adding a profile rewrote the
keys of existing mirrors and orphaned them. tkey() now uses the per-position
source profile; this script renames the existing state keys to match, resolving
each trade id against the bound portfolios' open and closed books.
"""
import sys
sys.path.insert(0, '.')
import copy_listener as cl
import source_api


def main():
    st = cl.load_state()
    idmap = {}
    try:
        book = cl.fetch_merged_books(cl.PROFILE)
        for p in book.get('positions', []):
            if p.get('profile') and p.get('source_id'):
                idmap[str(p['source_id'])] = p['profile']
    except Exception as e:
        print('open-book resolution failed:', type(e).__name__)
    try:
        for i, pid in enumerate(cl.PORTFOLIO_IDS):
            name = cl.PROFILE_NAMES[i] if i < len(cl.PROFILE_NAMES) else cl.PROFILE_NAMES[-1]
            for bid in source_api.fetch_closed_base_ids(name, pid, pages=2):
                idmap.setdefault(str(bid), name)
    except Exception as e:
        print('closed-book resolution failed:', type(e).__name__)

    def newkey(k):
        parts = str(k).split('|')
        if len(parts) != 3:
            return k
        sid = parts[2]
        prof = idmap.get(sid) or parts[0].split(',')[0]
        return prof + '|' + parts[1] + '|' + sid

    changed = []
    for section in ('mirrored', 'retained', 'skip_watch', 'source_missing', 'entry_blocks'):
        d = st.get(section)
        if isinstance(d, dict):
            for k in list(d.keys()):
                nk = newkey(k)
                if nk != k:
                    d[nk] = d.pop(k)
                    changed.append(k + '  ->  ' + nk)
                    rec = d[nk]
                    if isinstance(rec, dict) and 'key' in rec:
                        rec['key'] = nk
    cl.save_state(st)
    print(cl.NAME, 'renamed', len(changed), 'keys')
    for c in changed[:12]:
        print('  ', c)


main()
