#!/usr/bin/env python3
"""Match IYP Organization nodes to GLEIF LEI records.

Pipeline:
  1. Fetch Organization nodes from IYP (name, countries, PeeringDB/CAIDA IDs,
     websites, number of managed ASes).
     PeeringDB attributes (city, aka, name_long) are read from the raw org data
     IYP stores on the EXTERNAL_ID relationship, so no PeeringDB API access is needed.
  3. Download the GLEIF Level 1 golden copy (lei2) and build name indexes,
     blocked by country.
  4. Match in tiers of decreasing confidence; anything with several equally good
     candidates goes to ambiguous.csv instead of the mapping.
  5. Look up organizations that did not match well in the registries via RDAP
     (see whois_enrich.py), using the CAIDA whois handles / ASNs stored in IYP,
     and match again with the registered name, country, city and postal code.
  6. Apply manual overrides, write mapping.csv / ambiguous.csv / report.md.

The output is meant to be consumed by an IYP crawler as a curated dataset, so
precision matters more than recall.
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import sys
import unicodedata
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone

import pandas as pd
import requests
from rapidfuzz import fuzz, process

import whois_enrich

GLEIF_LATEST = 'https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest'

IYP_URI = os.environ.get('IYP_BOLT_URI', 'neo4j://iyp-bolt.ihr.live:7687')
IYP_USER = os.environ.get('IYP_USER', 'neo4j')
IYP_PASSWORD = os.environ.get('IYP_PASSWORD', 'password')

# Tier -> confidence. Keep these coarse; they are meant for filtering, not ranking.
CONFIDENCE = {
    'manual': 1.0,
    'legal_exact': 1.0,
    'other_name_exact': 0.95,
    'legal_core': 0.9,
    'other_name_core': 0.85,
    'fuzzy': 0.7,
    'legal_exact_nocountry': 0.5,
}
FUZZY_THRESHOLD = 92   # token_sort_ratio, 0-100
FUZZY_MARGIN = 3       # best must beat runner-up (different LEI) by this much

# Legal-form tokens stripped from the *end* of a normalized name to produce the
# "core" name. Multi-token entries are matched before single tokens. This list is
# deliberately limited to legal forms; words like "group" or "holding" carry
# identity and must stay. Extend from the ISO 20275 ELF code list if needed.
LEGAL_SUFFIXES = [
    # multi-token
    'pte ltd', 'pty ltd', 'co ltd', 'sdn bhd', 'sp z o o', 'sp z oo', 'a s', 's a',
    'd o o', 's r o', 's r l', 's a s', 's a r l', 'k k', 'fz llc', 'pvt ltd',
    'private limited', 'public limited company', 'limited liability company',
    'kabushiki kaisha', 'kabushiki gaisha', 'yugen kaisha', 'yugen gaisha',
    # single token
    'inc', 'incorporated', 'corp', 'corporation', 'co', 'company', 'ltd', 'limited',
    'llc', 'lp', 'llp', 'plc', 'pte', 'pty', 'sa', 'sas', 'sarl', 'srl', 'sl', 'sau',
    'spa', 'nv', 'bv', 'gmbh', 'ag', 'kg', 'kgaa', 'mbh', 'ug', 'ohg', 'se', 'ab',
    'as', 'asa', 'oy', 'oyj', 'aps', 'ltda', 'bhd', 'kk', 'oao', 'ooo', 'zao', 'pao',
    'doo', 'sro', 'ehf', 'hf', 'eood', 'ood', 'ad', 'tov', 'pjsc', 'jsc', 'cjsc',
    'ojsc', 'sae', 'wll', 'fze', 'fzco', 'dmcc', 'pvt', 'ltee', 'cia', 'eireli',
    'sapi', 'sapib', 'sccl', 'scrl', 'cvba', 'bvba', 'vof', 'cv', 'kft', 'zrt', 'nyrt',
    'bt', 'sp', 'spol', 'akciova spolecnost', 'aktiengesellschaft', 'aktiebolag',
    'aktieselskab', 'aksjeselskap', 'osakeyhtio', 'societe anonyme', 'sociedad anonima',
    'societa per azioni', 'naamloze vennootschap', 'besloten vennootschap',
    '株式会社', '有限会社', '合同会社', '有限公司', '股份有限公司',
]
_MULTI = [s.split() for s in LEGAL_SUFFIXES if ' ' in s]
_SINGLE = {s for s in LEGAL_SUFFIXES if ' ' not in s}

_PUNCT_RE = re.compile(r'[^\w\s]', re.UNICODE)
_WS_RE = re.compile(r'\s+')


# --------------------------------------------------------------------------- #
# Normalization
# --------------------------------------------------------------------------- #

def normalize(name: str) -> str:
    """Full normalization: casefold, strip diacritics, drop punctuation, collapse
    whitespace. Does NOT strip legal forms."""
    if not name:
        return ''
    s = unicodedata.normalize('NFKD', str(name))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.casefold().replace('&', ' and ')
    # CJK company suffixes are glued to the name; separate them so they can be
    # treated as tokens.
    for cjk in ('株式会社', '有限会社', '合同会社', '有限公司', '股份有限公司'):
        s = s.replace(cjk, f' {cjk} ')
    s = _PUNCT_RE.sub(' ', s)
    s = _WS_RE.sub(' ', s).strip()
    if s.startswith('the '):
        s = s[4:]
    return s


def core_name(norm: str) -> str:
    """Strip legal-form suffixes from the end of an already-normalized name."""
    tokens = norm.split()
    changed = True
    while changed and len(tokens) > 1:
        changed = False
        for multi in _MULTI:
            n = len(multi)
            if len(tokens) > n and tokens[-n:] == multi:
                tokens = tokens[:-n]
                changed = True
                break
        if not changed and len(tokens) > 1 and tokens[-1] in _SINGLE:
            tokens = tokens[:-1]
            changed = True
    return ' '.join(tokens)


def normalize_postcode(value: str) -> str:
    return re.sub(r'[\s-]', '', str(value or '')).upper()


def first_token(norm: str) -> str:
    return norm.split(' ', 1)[0] if norm else ''


def jurisdiction_cc(value: str) -> str:
    """GLEIF LegalJurisdiction is an ISO 3166-1 code, optionally with a
    subdivision (e.g. 'US-DE'). Return the country part."""
    if not value or pd.isna(value):
        return ''
    return str(value).split('-', 1)[0].strip().upper()


# --------------------------------------------------------------------------- #
# IYP
# --------------------------------------------------------------------------- #

# The peeringdb.org crawler stores the flattened raw PeeringDB org object as
# properties of the EXTERNAL_ID relationship, which gives us city, aka and
# name_long without touching the PeeringDB API.
IYP_QUERY = """
MATCH (o:Organization)
RETURN o.name AS name,
       [(o)-[r:EXTERNAL_ID]->(x:PeeringdbOrgID) |
        {id: x.id, city: r.city, aka: r.aka, name_long: r.name_long}] AS pdb,
       [(o)-[:EXTERNAL_ID]->(x:CaidaOrgID) | x.id] AS caida_ids,
       [(o)-[:COUNTRY]->(c:Country) | c.country_code] AS countries,
       [(o)-[:WEBSITE]->(u:URL) | u.url] AS websites,
       [(a:AS)-[:MANAGED_BY]->(o) | a.asn] AS asns
"""


def fetch_iyp_orgs() -> pd.DataFrame:
    from neo4j import GraphDatabase

    logging.info(f'Fetching Organization nodes from {IYP_URI}')
    rows = []
    with GraphDatabase.driver(IYP_URI, auth=(IYP_USER, IYP_PASSWORD)) as driver:
        with driver.session() as session:
            for rec in session.run(IYP_QUERY):
                pdb = rec['pdb'] or []
                alt = {(p.get('aka') or '').strip() for p in pdb} | {(p.get('name_long') or '').strip() for p in pdb}
                alt.discard('')
                alt.discard(rec['name'])
                rows.append({
                    'name': rec['name'],
                    'pdb_ids': sorted({str(p['id']) for p in pdb if p.get('id') is not None}),
                    'cities': sorted({normalize(p.get('city') or '') for p in pdb} - {''}),
                    'alt_names': sorted(alt),
                    'caida_ids': sorted({str(x) for x in rec['caida_ids'] if x is not None}),
                    'countries': sorted({str(x).upper() for x in rec['countries'] if x}),
                    'websites': sorted({str(x) for x in rec['websites'] if x}),
                    'asns': sorted({int(a) for a in rec['asns'] if a is not None}),
                    'n_as': len({a for a in rec['asns'] if a is not None}),
                })
    df = pd.DataFrame(rows)
    logging.info(f'Got {len(df)} organizations, {df.n_as.gt(0).sum()} with ASes, '
                 f'{df.countries.map(len).gt(0).sum()} with a country')
    return df


def load_iyp_orgs_csv(path: str) -> pd.DataFrame:
    """Offline alternative to fetch_iyp_orgs() for testing. Columns:
    name, pdb_ids, caida_ids, countries, websites, n_as, and optionally cities,
    alt_names and asns (all lists as ';'-separated)."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in ('pdb_ids', 'caida_ids', 'countries', 'websites', 'cities', 'alt_names', 'asns'):
        if col not in df:
            df[col] = ''
        df[col] = df[col].map(lambda v: [x for x in v.split(';') if x])
    df['cities'] = df['cities'].map(lambda l: [normalize(c) for c in l])
    df['countries'] = df['countries'].map(lambda l: [c.upper() for c in l])
    df['n_as'] = pd.to_numeric(df['n_as'], errors='coerce').fillna(0).astype(int)
    return df


# --------------------------------------------------------------------------- #
# GLEIF
# --------------------------------------------------------------------------- #

def download_gleif(cache_dir: str) -> tuple[str, str]:
    """Download the latest lei2 golden copy CSV zip. Returns (zip_path, publish_date)."""
    os.makedirs(cache_dir, exist_ok=True)
    publish = ''
    try:
        meta = requests.get(GLEIF_LATEST, timeout=60, headers={'Accept': 'application/json'})
        if meta.ok:
            j = meta.json()
            # Be lenient about the exact JSON layout; only the publish date is used.
            publish = str(j.get('data', j).get('publish_date', ''))[:10]
    except Exception as e:
        logging.warning(f'Could not read golden copy metadata: {e}')
    zip_path = os.path.join(cache_dir, f'gleif-lei2-{publish or "latest"}.csv.zip')
    if os.path.exists(zip_path) and os.path.getsize(zip_path) > 0:
        logging.info(f'Using cached {zip_path}')
        return zip_path, publish
    logging.info('Downloading GLEIF golden copy (this is a few hundred MB)...')
    with requests.get(GLEIF_LATEST + '.csv', stream=True, allow_redirects=True, timeout=600) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
    logging.info(f'Saved {zip_path} ({os.path.getsize(zip_path) / 2**20:.0f} MiB)')
    return zip_path, publish


class GleifIndex:
    """Name indexes over the golden copy, blocked by country.

    exact[(cc, norm_full)]  -> set(LEI)      (legal names)
    other[(cc, norm_full)]  -> set(LEI)      (other/transliterated names)
    core_legal[(cc, core)]  -> set(LEI)
    core_other[(cc, core)]  -> set(LEI)
    fuzzy_block[(cc, first_token)] -> list[(core, LEI)]   (legal + other names)
    records[LEI] -> (legal_name, countries, city, entity_status, reg_status, postcode)
    """

    def __init__(self):
        self.exact = defaultdict(set)
        self.other = defaultdict(set)
        self.core_legal = defaultdict(set)
        self.core_other = defaultdict(set)
        self.fuzzy_block = defaultdict(list)
        self.global_exact = defaultdict(set)
        self.records = {}

    def add(self, lei, legal_name, other_names, countries, city, entity_status, reg_status, postcode=''):
        self.records[lei] = (legal_name, tuple(sorted(countries)), city, entity_status, reg_status, postcode)
        ln = normalize(legal_name)
        lc = core_name(ln)
        self.global_exact[ln].add(lei)
        seen_fuzzy = set()
        for cc in countries:
            self.exact[(cc, ln)].add(lei)
            self.core_legal[(cc, lc)].add(lei)
            key = (cc, first_token(lc))
            if key not in seen_fuzzy:
                self.fuzzy_block[key].append((lc, lei))
                seen_fuzzy.add(key)
        for on in other_names:
            n = normalize(on)
            if not n or n == ln:
                continue
            c = core_name(n)
            for cc in countries:
                self.other[(cc, n)].add(lei)
                self.core_other[(cc, c)].add(lei)
                key = (cc, first_token(c))
                if (key, c) not in seen_fuzzy:
                    self.fuzzy_block[key].append((c, lei))
                    seen_fuzzy.add((key, c))


def _discover_columns(header: list[str]) -> dict:
    """The number of OtherEntityName columns varies between golden copy versions,
    so find them by prefix instead of hardcoding."""
    def pick(prefix):
        return [c for c in header if c.startswith(prefix) and not c.endswith(('.xmllang', '.type'))]

    cols = {
        'lei': 'LEI',
        'legal_name': 'Entity.LegalName',
        'other_names': pick('Entity.OtherEntityNames.OtherEntityName.')
        + pick('Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.'),
        'legal_country': 'Entity.LegalAddress.Country',
        'legal_city': 'Entity.LegalAddress.City',
        'legal_postcode': 'Entity.LegalAddress.PostalCode',
        'hq_country': 'Entity.HeadquartersAddress.Country',
        'jurisdiction': 'Entity.LegalJurisdiction',
        'entity_status': 'Entity.EntityStatus',
        'reg_status': 'Registration.RegistrationStatus',
    }
    missing = [v for k, v in cols.items() if k != 'other_names' and v not in header]
    if missing:
        raise RuntimeError(f'Golden copy is missing expected columns: {missing}')
    return cols


def load_gleif(zip_path: str) -> GleifIndex:
    index = GleifIndex()
    with zipfile.ZipFile(zip_path) as z:
        csv_name = next(n for n in z.namelist() if n.lower().endswith('.csv'))
        with z.open(csv_name) as fh:
            header = next(csv.reader(io.TextIOWrapper(fh, encoding='utf-8')))
        cols = _discover_columns(header)
        usecols = [cols['lei'], cols['legal_name'], cols['legal_country'], cols['legal_city'],
                   cols['legal_postcode'], cols['hq_country'], cols['jurisdiction'], cols['entity_status'],
                   cols['reg_status']] + cols['other_names']
        logging.info(f'Reading {csv_name} ({len(cols["other_names"])} other-name columns)')
        n = 0
        skipped = 0
        with z.open(csv_name) as fh:
            for chunk in pd.read_csv(fh, usecols=usecols, dtype=str, keep_default_na=False,
                                     chunksize=200_000):
                for row in chunk.itertuples(index=False):
                    # Dotted column names are mangled by _asdict(); zip by position instead.
                    r = dict(zip(chunk.columns, row))
                    reg = r[cols['reg_status']]
                    # ANNULLED = assigned in error, DUPLICATE = superseded. Never match these.
                    if reg in ('ANNULLED', 'DUPLICATE'):
                        skipped += 1
                        continue
                    countries = {jurisdiction_cc(r[cols['jurisdiction']]),
                                 r[cols['legal_country']].strip().upper(),
                                 r[cols['hq_country']].strip().upper()}
                    countries.discard('')
                    index.add(
                        lei=r[cols['lei']],
                        legal_name=r[cols['legal_name']],
                        other_names=[r[c] for c in cols['other_names'] if r[c]],
                        countries=countries,
                        city=normalize(r[cols['legal_city']]),
                        entity_status=r[cols['entity_status']],
                        reg_status=reg,
                        postcode=normalize_postcode(r[cols['legal_postcode']]),
                    )
                    n += 1
                logging.info(f'  ...{n} records indexed')
    logging.info(f'Indexed {n} LEI records ({skipped} annulled/duplicate skipped)')
    return index


# --------------------------------------------------------------------------- #
# Matching
# --------------------------------------------------------------------------- #

def _prefer(cands: set, index: GleifIndex, cities: list, postcodes: list = ()) -> tuple[set, str]:
    """Try to reduce a candidate set to one LEI. Returns (candidates, hint)."""
    if len(cands) <= 1:
        return cands, ''
    active = {l for l in cands if index.records[l][3] == 'ACTIVE'}
    if len(active) == 1:
        return active, 'active_only'
    if active:
        cands = active
    if postcodes:
        by_pc = {l for l in cands if index.records[l][5] and index.records[l][5] in postcodes}
        if len(by_pc) == 1:
            return by_pc, 'postcode'
    if cities:
        by_city = {l for l in cands if index.records[l][2] in cities}
        if len(by_city) == 1:
            return by_city, 'city'
    return cands, ''


def match_org(org, index: GleifIndex, whois: dict = None):
    """Return (lei, method, matched_name, hint) or (None, 'ambiguous'|'unmatched', cands, '').

    ``whois`` is the registry record from whois_enrich (name, country, city,
    postcode), if any. Its name is tried after the IYP and PeeringDB names, its
    country is used as an additional block when IYP has none matching, and city /
    postal code serve as tie-breakers.
    """
    whois = whois or {}
    norm = normalize(org.name)
    core = core_name(norm)
    if not norm:
        return None, 'unmatched', None, ''
    iyp_countries = list(org.countries or [])
    whois_countries = [whois['country']] if whois.get('country') and whois['country'] not in iyp_countries else []
    country_sets = [(iyp_countries, '')]
    if whois_countries:
        country_sets.append((whois_countries, 'whois_country'))
    cities = list(getattr(org, 'cities', []) or [])
    if whois.get('city'):
        cities.append(normalize(whois['city']))
    postcodes = [normalize_postcode(whois['postcode'])] if whois.get('postcode') else []
    # Query names: the IYP name first, then PeeringDB name_long / aka, then whois.
    query_names = [(norm, core, '')]
    for alt in getattr(org, 'alt_names', []) or []:
        an = normalize(alt)
        if an and an != norm:
            query_names.append((an, core_name(an), 'alt_name'))
    if whois.get('name'):
        wn = normalize(whois['name'])
        if wn and wn not in {q[0] for q in query_names}:
            query_names.append((wn, core_name(wn), 'whois_name'))

    tiers = [
        ('legal_exact', index.exact, 0),
        ('other_name_exact', index.other, 0),
        ('legal_core', index.core_legal, 1),
        ('other_name_core', index.core_other, 1),
    ]
    for method, idx, which in tiers:
        for qn in query_names:
            key, src = qn[which], qn[2]
            for countries, csrc in country_sets:
                cands = set()
                for cc in countries:
                    cands |= idx.get((cc, key), set())
                if not cands:
                    continue
                cands, hint = _prefer(cands, index, cities, postcodes)
                hint = ','.join(h for h in (src, csrc, hint) if h)
                if len(cands) == 1:
                    lei = next(iter(cands))
                    return lei, method, key, hint
                return None, 'ambiguous', sorted(cands), method

    countries = iyp_countries + whois_countries

    # Fuzzy, blocked on (country, first token of core name).
    if countries and len(core) >= 4:
        block = []
        for cc in countries:
            block.extend(index.fuzzy_block.get((cc, first_token(core)), []))
        if block:
            choices = [b[0] for b in block]
            hits = process.extract(core, choices, scorer=fuzz.token_sort_ratio,
                                   limit=5, score_cutoff=FUZZY_THRESHOLD)
            if hits:
                best_score = hits[0][1]
                best_leis = {block[h[2]][1] for h in hits if h[1] == best_score}
                runner_up = max((h[1] for h in hits if block[h[2]][1] not in best_leis), default=0)
                best_leis, hint = _prefer(best_leis, index, cities, postcodes)
                if len(best_leis) == 1 and best_score - runner_up >= FUZZY_MARGIN:
                    lei = next(iter(best_leis))
                    if whois_countries and not iyp_countries:
                        hint = ','.join(h for h in ('whois_country', hint) if h)
                    return lei, 'fuzzy', hits[0][0], f'score={best_score:.0f}{"," + hint if hint else ""}'
                return None, 'ambiguous', sorted(best_leis), 'fuzzy'

    # No country in IYP: only accept a globally unique exact legal-name match.
    if not countries:
        cands = index.global_exact.get(norm, set())
        cands, hint = _prefer(cands, index, cities, postcodes)
        if len(cands) == 1:
            return next(iter(cands)), 'legal_exact_nocountry', norm, hint
        if len(cands) > 1:
            return None, 'ambiguous', sorted(cands), 'legal_exact_nocountry'

    return None, 'unmatched', None, ''


def apply_overrides(mapping: pd.DataFrame, path: str, orgs: pd.DataFrame,
                    index: GleifIndex) -> pd.DataFrame:
    """overrides CSV columns: iyp_org_name, lei, action (accept|reject)."""
    if not os.path.exists(path):
        return mapping
    ov = pd.read_csv(path, dtype=str, keep_default_na=False, comment='#')
    if ov.empty:
        return mapping
    rejects = {(r.iyp_org_name, r.lei) for r in ov.itertuples() if r.action == 'reject'}
    reject_names = {r.iyp_org_name for r in ov.itertuples() if r.action == 'reject' and not r.lei}
    before = len(mapping)
    mapping = mapping[~mapping.apply(lambda r: (r.iyp_org_name, r.lei) in rejects
                                     or r.iyp_org_name in reject_names, axis=1)]
    logging.info(f'Overrides: rejected {before - len(mapping)} matches')

    org_by_name = orgs.set_index('name')
    accepted = []
    for r in ov[ov.action == 'accept'].itertuples():
        if r.iyp_org_name not in org_by_name.index or r.lei not in index.records:
            logging.warning(f'Override ignored (unknown org or LEI): {r.iyp_org_name} -> {r.lei}')
            continue
        # A manual accept replaces whatever the matcher produced for this org.
        mapping = mapping[mapping.iyp_org_name != r.iyp_org_name]
        accepted.append(_row(org_by_name.loc[r.iyp_org_name], r.iyp_org_name, r.lei,
                             'manual', index.records[r.lei][0], 'override', index))
    if accepted:
        mapping = pd.concat([mapping, pd.DataFrame(accepted)], ignore_index=True)
        logging.info(f'Overrides: accepted {len(accepted)} manual matches')
    return mapping


def _row(org, name, lei, method, matched_name, hint, index: GleifIndex) -> dict:
    legal_name, countries, city, entity_status, reg_status, _postcode = index.records[lei]
    return {
        'iyp_org_name': name,
        'lei': lei,
        'lei_legal_name': legal_name,
        'lei_countries': ';'.join(countries),
        'entity_status': entity_status,
        'registration_status': reg_status,
        'match_method': method,
        'confidence': CONFIDENCE[method],
        'matched_name': matched_name,
        'hint': hint,
        'iyp_countries': ';'.join(org.countries),
        'peeringdb_org_ids': ';'.join(org.pdb_ids),
        'caida_org_ids': ';'.join(org.caida_ids),
        'iyp_websites': ';'.join(org.websites),
        'iyp_n_as': org.n_as,
    }


def run_matching(orgs: pd.DataFrame, index: GleifIndex, whois: dict = None):
    whois = whois or {}
    matched, ambiguous = [], []
    stats = Counter()
    for org in orgs.itertuples(index=False):
        lei, method, extra, hint = match_org(org, index, whois.get(org.name))
        stats[method] += 1
        if lei:
            matched.append(_row(org, org.name, lei, method, extra, hint, index))
        elif method == 'ambiguous':
            ambiguous.append({
                'iyp_org_name': org.name,
                'iyp_countries': ';'.join(org.countries),
                'iyp_n_as': org.n_as,
                'tier': hint,
                'candidates': ';'.join(extra),
                'candidate_names': ' | '.join(f'{index.records[l][0]} [{index.records[l][3]}]' for l in extra),
            })
    mapping = pd.DataFrame(matched)
    if not mapping.empty:
        mapping = mapping.sort_values(['confidence', 'iyp_n_as', 'iyp_org_name'],
                                      ascending=[False, False, True])
    return mapping, pd.DataFrame(ambiguous), stats


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #

def write_report(path, orgs, mapping, ambiguous, stats, publish_date, whois=None):
    whois = whois or {}
    total = len(orgs)
    with_as = orgs[orgs.n_as > 0]
    as_total = orgs.n_as.sum()
    matched_names = set(mapping.iyp_org_name) if not mapping.empty else set()
    as_covered = orgs[orgs.name.isin(matched_names)].n_as.sum()
    lines = [
        '# IYP ↔ GLEIF matching report', '',
        f'- Generated: {datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}',
        f'- GLEIF golden copy publish date: {publish_date or "unknown"}',
        f'- IYP organizations: {total} ({len(with_as)} manage at least one AS)',
        f'- Organizations with a country in IYP: {orgs.countries.map(len).gt(0).sum()}', '',
        '## Results', '',
        f'- Matched: **{len(matched_names)}** ({len(matched_names) / max(total, 1):.1%} of orgs)',
        f'- Ambiguous (needs manual review): {len(ambiguous)}',
        f'- Unmatched: {stats.get("unmatched", 0)}',
        f'- AS coverage: {as_covered} of {as_total} AS→Organization links '
        f'({as_covered / max(as_total, 1):.1%}) point to a matched organization', '',
        '## Matches by method', '', '| method | confidence | count |', '|---|---|---|',
    ]
    if whois:
        n_whois_hint = int(mapping.hint.fillna('').str.contains('whois').sum()) if not mapping.empty else 0
        lines[lines.index('## Results')-1:lines.index('## Results')-1] = [
            f'- Organizations with registry (RDAP) data: {len(whois)}; '
            f'matches that needed it: {n_whois_hint} (see `hint` column)',
        ]
    if not mapping.empty:
        for method, cnt in mapping.match_method.value_counts().items():
            lines.append(f'| {method} | {CONFIDENCE[method]} | {cnt} |')
        lines += ['', '## Matched organizations by country (top 20)', '', '| country | count |', '|---|---|']
        cc = Counter()
        for v in mapping.iyp_countries:
            for c in v.split(';'):
                if c:
                    cc[c] += 1
        for c, n in cc.most_common(20):
            lines.append(f'| {c} | {n} |')
        lines += ['', '## Largest matched organizations (by number of ASes)', '',
                  '| organization | LEI | legal name | method |', '|---|---|---|---|']
        for r in mapping.sort_values('iyp_n_as', ascending=False).head(25).itertuples():
            lines.append(f'| {r.iyp_org_name} | {r.lei} | {r.lei_legal_name} | {r.match_method} |')
    if not ambiguous.empty:
        lines += ['', '## Largest ambiguous organizations (review these first)', '',
                  '| organization | ASes | candidates |', '|---|---|---|']
        for r in ambiguous.sort_values('iyp_n_as', ascending=False).head(25).itertuples():
            lines.append(f'| {r.iyp_org_name} | {r.iyp_n_as} | {r.candidate_names} |')
    with open(path, 'w') as f:
        f.write('\n'.join(lines) + '\n')


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--out-dir', default='data')
    p.add_argument('--cache-dir', default='cache')
    p.add_argument('--overrides', default='overrides/manual_overrides.csv')
    p.add_argument('--gleif-zip', help='Use a local golden copy zip instead of downloading')
    p.add_argument('--iyp-csv', help='Read organizations from CSV instead of querying IYP')
    p.add_argument('--write-unmatched', action='store_true', help='Also write unmatched.csv')
    p.add_argument('--no-whois', action='store_true', help='Skip registry (RDAP) enrichment')
    p.add_argument('--rdap-cache', default='cache/rdap_cache.json',
                   help='Persistent cache of RDAP lookups (keep it between runs)')
    p.add_argument('--rdap-budget', type=int, default=int(os.environ.get('RDAP_BUDGET', 3000)),
                   help='Maximum number of new RDAP requests per run')
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S')
    os.makedirs(args.out_dir, exist_ok=True)

    orgs = load_iyp_orgs_csv(args.iyp_csv) if args.iyp_csv else fetch_iyp_orgs()
    orgs = orgs[orgs.name.notna() & (orgs.name.str.strip() != '')].copy()

    if args.gleif_zip:
        zip_path, publish_date = args.gleif_zip, ''
    else:
        zip_path, publish_date = download_gleif(args.cache_dir)
    index = load_gleif(zip_path)

    logging.info('Matching...')
    mapping, ambiguous, stats = run_matching(orgs, index)
    logging.info(f'Matching stats: {dict(stats)}')

    whois = {}
    if not args.no_whois:
        # Spend the RDAP budget on organizations that are unmatched, ambiguous or
        # matched with low confidence, largest first; cached lookups are free.
        good = set(mapping[mapping.confidence >= 0.9].iyp_org_name) if not mapping.empty else set()
        priority = set(orgs.name) - good
        whois = whois_enrich.enrich(orgs, args.rdap_cache, args.rdap_budget, priority)
        if whois:
            logging.info('Matching again with registry data...')
            mapping, ambiguous, stats = run_matching(orgs, index, whois)
            logging.info(f'Matching stats: {dict(stats)}')

    mapping = apply_overrides(mapping, args.overrides, orgs, index)

    mapping.to_csv(os.path.join(args.out_dir, 'iyp_gleif_mapping.csv'), index=False)
    ambiguous.to_csv(os.path.join(args.out_dir, 'ambiguous.csv'), index=False)
    if args.write_unmatched:
        matched = set(mapping.iyp_org_name) if not mapping.empty else set()
        amb = set(ambiguous.iyp_org_name) if not ambiguous.empty else set()
        um = orgs[~orgs.name.isin(matched | amb)][['name', 'countries', 'n_as']].copy()
        um['countries'] = um.countries.map(';'.join)
        um.sort_values('n_as', ascending=False).to_csv(os.path.join(args.out_dir, 'unmatched.csv'), index=False)
    write_report(os.path.join(args.out_dir, 'report.md'), orgs, mapping, ambiguous, stats, publish_date, whois)
    with open(os.path.join(args.out_dir, 'metadata.json'), 'w') as f:
        json.dump({
            'generated': datetime.now(tz=timezone.utc).isoformat(),
            'gleif_publish_date': publish_date,
            'iyp_uri': None if args.iyp_csv else IYP_URI,
            'n_orgs': int(len(orgs)),
            'n_matched': int(len(mapping)),
            'n_ambiguous': int(len(ambiguous)),
            'n_whois_enriched': len(whois),
            'fuzzy_threshold': FUZZY_THRESHOLD,
        }, f, indent=2)
    logging.info(f'Wrote {len(mapping)} matches and {len(ambiguous)} ambiguous cases to {args.out_dir}/')


if __name__ == '__main__':
    main()
    sys.exit(0)
