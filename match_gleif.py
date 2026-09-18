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
    # AS names describe a network, not a legal entity, so they rank below every name
    # that is attached to the organization itself.
    'as_name_exact': 0.85,
    'as_name_core': 0.75,
    'fuzzy': 0.7,
    'address_name': 0.6,
    'legal_exact_nocountry': 0.5,
}
FUZZY_THRESHOLD = 92   # token_sort_ratio, 0-100
FUZZY_MARGIN = 3       # best must beat runner-up (different LEI) by this much
# address_name tier: same (country, postal code, street number) as GLEIF's HQ address
# AND some name evidence. Addresses shared by more than ADDRESS_BLOCK_MAX entities
# (registered agents, carrier hotels, law firms) are never used.
ADDRESS_NAME_MIN_RATIO = 60
ADDRESS_BLOCK_MAX = 20
# Tokens that do not count as name evidence for the address tier.
GENERIC_TOKENS = {
    'the', 'and', 'of', 'for', 'de', 'la', 'le', 'du', 'des', 'der', 'die', 'das', 'und',
    'communications', 'communication', 'telecom', 'telecommunications', 'telecommunication',
    'network', 'networks', 'internet', 'services', 'service', 'solutions', 'systems', 'technology',
    'technologies', 'digital', 'data', 'cloud', 'hosting', 'online', 'media', 'global', 'international',
    'group', 'holding', 'holdings', 'company', 'enterprises', 'enterprise', 'industries', 'partners',
    'capital', 'management', 'consulting', 'net', 'com', 'inc', 'corp', 'ltd', 'llc', 'gmbh', 'sa', 'ag',
    'bv', 'nv', 'plc', 'co', 'usa', 'us', 'uk', 'europe', 'america', 'asia', 'pacific', 'north', 'south',
    'east', 'west', 'new', 'one', 'first', 'united', 'national', 'general', 'american', 'european',
}

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

# Generic words that may dominate the tail of names under some ELF codes (trusts,
# funds, associations) but carry identity; never learn them as legal forms.
_NEVER_LEARN = {
    'trust', 'fund', 'funds', 'bank', 'group', 'holding', 'holdings', 'partners',
    'capital', 'association', 'foundation', 'society', 'university', 'church',
    'council', 'authority', 'agency', 'international', 'services', 'systems',
    'network', 'networks', 'communications', 'telecom', 'technologies', 'solutions',
}


def add_legal_suffixes(suffixes):
    """Extend the suffix lists (e.g. with forms learned from the golden copy)."""
    for suf in suffixes:
        toks = suf.split()
        if not toks:
            continue
        if len(toks) == 1:
            _SINGLE.add(toks[0])
        elif toks not in _MULTI:
            _MULTI.append(toks)
    # Longest multi-token forms first so "gmbh and co kg" beats "co kg".
    _MULTI.sort(key=len, reverse=True)


def learn_legal_forms(tails_by_code: dict, min_count: int = 50, min_share: float = 0.2) -> dict:
    """Learn legal-form suffixes from the golden copy.

    ``tails_by_code`` maps an ISO 20275 ELF code to a Counter of (tail_length,
    tail) over the normalized legal names registered under that code. For each code
    with enough names, every tail (1-4 tokens) that ends at least ``min_share`` of
    the names is a legal form for that jurisdiction: this recovers "spolka
    akcyjna", "aktiebolag", "sp z o o", "pvt ltd", "co ltd", "kabushiki kaisha",
    "gmbh and co kg" and their local variants without a hand-written list.
    Returns {code: [suffixes]}.
    """
    learned = {}
    for code, (n_names, tails) in tails_by_code.items():
        if n_names < min_count:
            continue
        found = []
        for (length, tail), cnt in tails.most_common():
            if cnt / n_names < min_share:
                break
            toks = tail.split()
            if any(t in _NEVER_LEARN or t.isdigit() for t in toks):
                continue
            if length == 1 and len(toks[0]) < 2:
                continue
            found.append(tail)
        if found:
            learned[code] = found
    return learned

_PUNCT_RE = re.compile(r'[^\w\s]', re.UNICODE)
_PAREN_RE = re.compile(r'\s*[\(\[][^\)\]]*[\)\]]')
_WS_RE = re.compile(r'\s+')


# --------------------------------------------------------------------------- #
# Normalization
# --------------------------------------------------------------------------- #

def normalize(name: str, keep_paren: bool = False) -> str:
    """Full normalization: casefold, strip diacritics, drop punctuation, collapse
    whitespace. Does NOT strip legal forms.

    ``keep_paren`` keeps the content of parentheses, which is used as a tie-break:
    two candidates that differ only by a parenthetical ("Swisscom AG" vs "Swisscom
    (Schweiz) AG") are indistinguishable otherwise.
    """
    if not name:
        return ''
    s = unicodedata.normalize('NFKD', str(name))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    # Drop parentheticals ("Vodafone Idea Ltd. (VIL)", "SingTel (Internet Exchange)")
    # unless that would leave nothing.
    if not keep_paren:
        stripped = _PAREN_RE.sub('', s).strip()
        if stripped:
            s = stripped
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


def split_legal_form(norm: str) -> tuple[str, str]:
    """Split an already-normalized name into (core, legal-form suffix)."""
    tokens = norm.split()
    form = []
    changed = True
    while changed and len(tokens) > 1:
        changed = False
        for multi in _MULTI:
            n = len(multi)
            if len(tokens) > n and tokens[-n:] == multi:
                form = tokens[-n:] + form
                tokens = tokens[:-n]
                changed = True
                break
        if not changed and len(tokens) > 1 and tokens[-1] in _SINGLE:
            form = tokens[-1:] + form
            tokens = tokens[:-1]
            changed = True
    return ' '.join(tokens), ' '.join(form)


def core_name(norm: str) -> str:
    """Strip legal-form suffixes from the end of an already-normalized name."""
    return split_legal_form(norm)[0]


# Legal forms that denote the same kind of entity, so "Deutsche Bank AG" and
# "Deutsche Bank Aktiengesellschaft" agree while "Deutsche Bank Stiftung" does not.
# Only used to break ties between same-core candidates; anything not listed is its
# own class.
_FORM_CLASSES = {
    'AG': ['ag', 'aktiengesellschaft', 'sa', 's a', 'societe anonyme', 'sociedad anonima', 'spa',
           'societa per azioni', 'spolka akcyjna', 'społka akcyjna', 'nv', 'n v', 'naamloze vennootschap',
           'plc', 'public limited company', 'ab', 'aktiebolag', 'asa', 'oyj', 'as', 'a s', 'aksjeselskap',
           'aktieselskab', 'kk', 'kabushiki kaisha', 'kabushiki gaisha', '株式会社', 'ad', 'pao', 'pjsc', 'jsc'],
    'LTD': ['ltd', 'limited', 'co ltd', 'company limited', 'pty ltd', 'pty limited', 'pte ltd', 'pvt ltd',
            'private limited', 'sdn bhd', 'ltda', 'gmbh', 'mbh', 'ug', 'sarl', 's a r l', 'srl', 's r l', 'sl',
            'bv', 'b v', 'besloten vennootschap', 'sp z o o', 'sp z oo', 'spolka z ograniczona odpowiedzialnoscia',
            'oy', 'aps', 'sro', 's r o', 'doo', 'd o o', 'ooo', 'llc', 'l l c', 'limited liability company',
            'eood', 'ood', 'kft', 'tov', 'sas', 's a s', 'sau'],
    'INC': ['inc', 'incorporated', 'corp', 'corporation', 'co', 'company'],
    'LP': ['lp', 'llp', 'kg', 'gmbh and co kg', 'co kg', 'and co kg', 'cv', 'c v', 'sc', 'scs'],
}
_FORM_CLASS = {form: cls for cls, forms in _FORM_CLASSES.items() for form in forms}


def legal_form_class(form: str) -> str:
    """Canonical class of a legal-form suffix ('' if none); unknown forms map to themselves."""
    if not form:
        return ''
    if form in _FORM_CLASS:
        return _FORM_CLASS[form]
    # Multi-token forms such as "gmbh and co kg" are classified by their last known part.
    toks = form.split()
    for i in range(len(toks)):
        sub = ' '.join(toks[i:])
        if sub in _FORM_CLASS:
            return _FORM_CLASS[sub]
    return form


# AS names come from RIPE's asnames file (where the value is the aut-num handle
# followed by its descr, e.g. "DTAG Deutsche Telekom AG"), bgp.tools, CAIDA and
# PeeringDB. They name a *network*, so they need filtering before being used as
# entity names.
# A leading handle is uppercase (DTAG, AS3320, FR-RENATER, LEVEL3-ASN).
_AS_HANDLE_RE = re.compile(r'^[A-Z0-9][A-Z0-9._-]*$')
_AS_NAME_JUNK = {
    'err as name not found', 'unknown', 'unassigned', 'reserved', 'private', 'not assigned',
    'none', 'na', 'n a', 'null', 'test', 'default', 'customer', 'internet', 'network',
    'no name', 'noname', 'private customer', 'dummy',
}


def as_name_variants(raw: str) -> list:
    """Candidate entity names derived from one AS name.

    RIPE's value starts with the aut-num handle, so the name without its first token
    is offered as well ("DTAG Deutsche Telekom AG" -> also "Deutsche Telekom AG").
    The first token is only dropped when it is uppercase and the rest is not (a
    handle followed by a descr), and both variants are kept because the guess can be
    wrong ("NTT Communications Corporation"). plausible_entity_name() then discards
    the variants that are too generic to match on.
    """
    raw = str(raw or '').strip()
    tokens = raw.split()
    if len(tokens) < 2:
        # A single token is a handle or a brand ("AS-COGENT", "GOOGLE"), never a
        # legal entity name we can match on.
        return []
    out = [raw]
    if (len(tokens) >= 3 and _AS_HANDLE_RE.match(tokens[0])
            and any(c.islower() for c in ' '.join(tokens[1:]))):
        out.append(' '.join(tokens[1:]))
    return out


def plausible_entity_name(norm: str) -> bool:
    """Whether a normalized AS name can plausibly be a legal entity name.

    Rejects handles, single tokens and names made only of generic words: matching
    GLEIF on "GOOGLE", "AS-COGENT" or "Communications Corporation" (what is left of
    "NTT Communications Corporation" once its handle guess is stripped) would
    produce confident nonsense.
    """
    if not norm or len(norm) < 5 or norm in _AS_NAME_JUNK:
        return False
    if len(norm.split()) < 2:
        return False
    core, _form = split_legal_form(norm)
    # Needs at least one word that is neither a legal form, a generic industry word
    # nor a number: "Deutsche Telekom AG" qualifies, "Communications Corporation"
    # and "AS 12345" do not.
    return any(not t.isdigit() for t in distinctive_tokens(core))


def normalize_postcode(value: str) -> str:
    return re.sub(r'[\s-]', '', str(value or '')).upper()


_STREET_NUMBER_RE = re.compile(r'(?<![\w-])(\d{1,6})(?![\w-]*\d{3,})')
_UNIT_WORDS = {'suite', 'ste', 'unit', 'floor', 'fl', 'level', 'apt', 'room', 'rm', 'office', 'bldg', 'building', 'no', 'nr'}


def street_number(line: str) -> str:
    """First house/street number in an address line, skipping unit/floor numbers.
    '1025 Eldorado Blvd., Suite 400' -> 1025; 'Landgrabenweg 151' -> 151;
    'Suite 400, 1 Canada Square' -> 1; 'PO Box 123' -> 123 (a PO box is a fine key too)."""
    if not line:
        return ''
    norm = normalize(line)
    toks = norm.split()
    for i, tok in enumerate(toks):
        m = re.match(r'^(\d{1,6})[a-z]?$', tok)
        if not m:
            m = re.match(r'^(\d{1,6})-\d{1,6}$', tok)  # "12-14 Main St", "2-3-1 Otemachi"
        if m and not (i > 0 and toks[i - 1] in _UNIT_WORDS):
            return m.group(1).lstrip('0') or '0'
    return ''


def address_key(country: str, postcode: str, line: str):
    """Blocking key for the address tier, or None if any part is missing."""
    cc = (country or '').strip().upper()
    pc = normalize_postcode(postcode)
    num = street_number(line)
    if len(cc) != 2 or len(pc) < 3 or not num:
        return None
    return (cc, pc, num)


def distinctive_tokens(core: str) -> set:
    return {t for t in core.split() if len(t) >= 3 and t not in GENERIC_TOKENS and t not in _SINGLE}


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
        {id: x.id, city: r.city, aka: r.aka, name_long: r.name_long,
         address1: r.address1, zipcode: r.zipcode, country: r.country}] AS pdb,
       [(o)-[:EXTERNAL_ID]->(x:CaidaOrgID) | x.id] AS caida_ids,
       [(o)-[:COUNTRY]->(c:Country) | c.country_code] AS countries,
       [(o)-[:WEBSITE]->(u:URL) | u.url] AS websites,
       [(a:AS)-[:MANAGED_BY]->(o) | a.asn] AS asns,
       [(a:AS)-[:MANAGED_BY]->(o) | [(a)-[:NAME]->(n:Name) | n.name]] AS as_names
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
                    'addresses': sorted({k for k in (address_key(p.get('country'), p.get('zipcode'), p.get('address1'))
                                               for p in pdb) if k}),
                    'caida_ids': sorted({str(x) for x in rec['caida_ids'] if x is not None}),
                    'countries': sorted({str(x).upper() for x in rec['countries'] if x}),
                    'websites': sorted({str(x) for x in rec['websites'] if x}),
                    'asns': sorted({int(a) for a in rec['asns'] if a is not None}),
                    'as_names': sorted({str(n).strip() for names in (rec['as_names'] or [])
                                        for n in (names or []) if n and str(n).strip()}),
                    'n_as': len({a for a in rec['asns'] if a is not None}),
                })
    df = pd.DataFrame(rows)
    logging.info(f'Got {len(df)} organizations, {df.n_as.gt(0).sum()} with ASes, '
                 f'{df.countries.map(len).gt(0).sum()} with a country')
    return df


def load_iyp_orgs_csv(path: str) -> pd.DataFrame:
    """Offline alternative to fetch_iyp_orgs() for testing. Columns:
    name, pdb_ids, caida_ids, countries, websites, n_as, and optionally cities,
    alt_names, asns and as_names (all lists as ';'-separated)."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for col in ('pdb_ids', 'caida_ids', 'countries', 'websites', 'cities', 'alt_names', 'asns', 'as_names'):
        if col not in df:
            df[col] = ''
        df[col] = df[col].map(lambda v: [x for x in v.split(';') if x])
    df['cities'] = df['cities'].map(lambda l: [normalize(c) for c in l])
    # Optional 'addresses' column: 'CC|postcode|street line' entries separated by ';'.
    if 'addresses' not in df:
        df['addresses'] = ''
    df['addresses'] = df['addresses'].map(
        lambda v: [k for k in (address_key(*a.split('|', 2)) for a in v.split(';') if a.count('|') == 2) if k])
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
    records[LEI] -> (legal_name, countries, city, entity_status, reg_status, postcode,
                     category, legal_country, jurisdiction, legal_form, address_text)
    category is GLEIF's EntityCategory (GENERAL, BRANCH, FUND, ...). Branches share the
    head office's legal name, which is the main source of same-name candidates.
    """

    def __init__(self):
        self.exact = defaultdict(set)
        self.other = defaultdict(set)
        self.core_legal = defaultdict(set)
        self.core_other = defaultdict(set)
        self.fuzzy_block = defaultdict(list)
        self.global_exact = defaultdict(set)
        # (country, postcode, street number) of the HQ address (legal address as
        # fallback) -> LEIs. Crowded keys are dropped after loading.
        self.address = defaultdict(set)
        self.records = {}

    def add(self, lei, legal_name, other_names, countries, city, entity_status, reg_status, postcode='',
            category='', legal_country='', jurisdiction='', address=None, address_text=''):
        ln = normalize(legal_name)
        lc, form = split_legal_form(ln)
        self.records[lei] = (legal_name, tuple(sorted(countries)), city, entity_status, reg_status, postcode,
                             category, legal_country, jurisdiction, legal_form_class(form), address_text)
        if address:
            self.address[address].add(lei)
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
        'legal_line': 'Entity.LegalAddress.FirstAddressLine',
        'hq_line': 'Entity.HeadquartersAddress.FirstAddressLine',
        'hq_postcode': 'Entity.HeadquartersAddress.PostalCode',
        'hq_city': 'Entity.HeadquartersAddress.City',
        'hq_country': 'Entity.HeadquartersAddress.Country',
        'jurisdiction': 'Entity.LegalJurisdiction',
        'entity_status': 'Entity.EntityStatus',
        'reg_status': 'Registration.RegistrationStatus',
        'elf': 'Entity.LegalForm.EntityLegalFormCode',
        'category': 'Entity.EntityCategory',
    }
    missing = [v for k, v in cols.items() if k != 'other_names' and v not in header]
    if missing:
        raise RuntimeError(f'Golden copy is missing expected columns: {missing}')
    return cols


def learn_legal_forms_from_golden_copy(z: zipfile.ZipFile, csv_name: str, cols: dict) -> dict:
    """First pass over the golden copy: collect name tails per ELF code."""
    logging.info('Learning legal forms from ELF codes...')
    tails_by_code = {}
    with z.open(csv_name) as fh:
        for chunk in pd.read_csv(fh, usecols=[cols['legal_name'], cols['elf']], dtype=str,
                                 keep_default_na=False, chunksize=500_000):
            for name, code in zip(chunk[cols['legal_name']], chunk[cols['elf']]):
                if not code or len(code) != 4:
                    continue
                toks = normalize(name).split()
                if len(toks) < 2:
                    continue
                entry = tails_by_code.setdefault(code, [0, Counter()])
                entry[0] += 1
                for length in range(1, min(4, len(toks) - 1) + 1):
                    entry[1][(length, ' '.join(toks[-length:]))] += 1
    learned = learn_legal_forms({k: tuple(v) for k, v in tails_by_code.items()})
    n = sum(len(v) for v in learned.values())
    logging.info(f'Learned {n} legal-form suffixes from {len(learned)} ELF codes')
    return learned


def load_gleif(zip_path: str, learned_forms_path: str = None) -> GleifIndex:
    index = GleifIndex()
    with zipfile.ZipFile(zip_path) as z:
        csv_name = next(n for n in z.namelist() if n.lower().endswith('.csv'))
        with z.open(csv_name) as fh:
            header = next(csv.reader(io.TextIOWrapper(fh, encoding='utf-8')))
        cols = _discover_columns(header)
        learned = learn_legal_forms_from_golden_copy(z, csv_name, cols)
        add_legal_suffixes(suf for sufs in learned.values() for suf in sufs)
        if learned_forms_path:
            with open(learned_forms_path, 'w') as f:
                json.dump(learned, f, indent=1, ensure_ascii=False, sort_keys=True)
        usecols = [cols['lei'], cols['legal_name'], cols['legal_country'], cols['legal_city'],
                   cols['legal_postcode'], cols['hq_country'], cols['category'], cols['jurisdiction'], cols['entity_status'],
                   cols['reg_status'], cols['legal_line'], cols['hq_line'], cols['hq_postcode'], cols['hq_city']
                   ] + cols['other_names']
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
                    # HQ address first: the legal address of US entities is often a
                    # registered agent shared by thousands of LEIs.
                    hq_cc = r[cols['hq_country']].strip().upper()
                    addr = address_key(hq_cc, r[cols['hq_postcode']], r[cols['hq_line']])
                    addr_text = f"{r[cols['hq_line']]}, {r[cols['hq_postcode']]} {r[cols['hq_city']]}, {hq_cc}"
                    if not addr:
                        addr = address_key(r[cols['legal_country']], r[cols['legal_postcode']], r[cols['legal_line']])
                        addr_text = (f"{r[cols['legal_line']]}, {r[cols['legal_postcode']]} {r[cols['legal_city']]}, "
                                     f"{r[cols['legal_country']].strip().upper()}")
                    index.add(
                        lei=r[cols['lei']],
                        legal_name=r[cols['legal_name']],
                        other_names=[r[c] for c in cols['other_names'] if r[c]],
                        countries=countries,
                        city=normalize(r[cols['legal_city']]),
                        entity_status=r[cols['entity_status']],
                        reg_status=reg,
                        postcode=normalize_postcode(r[cols['legal_postcode']]),
                        category=r[cols['category']],
                        legal_country=r[cols['legal_country']].strip().upper(),
                        jurisdiction=jurisdiction_cc(r[cols['jurisdiction']]),
                        address=addr,
                        address_text=addr_text if addr else '',
                    )
                    n += 1
                logging.info(f'  ...{n} records indexed')
    crowded = [k for k, v in index.address.items() if len(v) > ADDRESS_BLOCK_MAX]
    for k in crowded:
        del index.address[k]
    logging.info(f'Indexed {n} LEI records ({skipped} annulled/duplicate skipped); '
                 f'{len(index.address)} usable HQ addresses ({len(crowded)} crowded addresses dropped)')
    return index


# --------------------------------------------------------------------------- #
# Matching
# --------------------------------------------------------------------------- #

def _prefer(cands: set, index: GleifIndex, cities: list, postcodes: list = (),
            form_class: str = '', countries: tuple = (), raw_norm: str = '') -> tuple[set, str]:
    """Try to reduce a candidate set to one LEI. Returns (candidates, hint).

    Same-name candidates are mostly (a) a head office plus its international branches,
    which GLEIF registers under the head office's legal name, (b) the same core name
    under different legal forms, (c) a foreign subsidiary of the same group that is
    headquartered in the query country (the country block is the union of legal
    jurisdiction, legal address and HQ country, so e.g. an Ontario-registered entity
    with a New Jersey head office competes for a US organization), or (d) unrelated
    same-name entities (e.g. several "United Community Bank"s). Tie-breaks, in order:
    active status; the name that also matches with parentheticals kept ("Swisscom
    (Schweiz) AG" over "Swisscom AG"); same legal-form class as the query ("AG" vs
    "Aktiengesellschaft" agree, "Stiftung" does not); GENERAL entities over
    BRANCH/FUND/...; the head office (legal address in the jurisdiction country)
    over foreign branches;
    registration in one of the query countries over a foreign registration; a current
    registration over a lapsed one; postal code; city. Case (d) stays ambiguous unless
    a postal code or city is known.

    ``countries`` are the countries the candidates were looked up under, and are
    compared against each candidate's legal jurisdiction. Jurisdiction is checked
    before registration status because a lapsed record of the right entity beats a
    current record of a foreign namesake. ``raw_norm`` is the query name normalized
    with parentheticals kept.
    """
    if len(cands) <= 1:
        return cands, ''
    hints = []

    def narrow(subset, hint):
        nonlocal cands
        if subset and len(subset) < len(cands):
            cands = subset
            hints.append(hint)
        return len(cands) == 1

    if narrow({l for l in cands if index.records[l][3] == 'ACTIVE'}, 'active_only'):
        return cands, ','.join(hints)
    if raw_norm and narrow({l for l in cands if normalize(index.records[l][0], keep_paren=True) == raw_norm},
                           'exact_paren'):
        return cands, ','.join(hints)
    if form_class and narrow({l for l in cands if index.records[l][9] == form_class}, 'legal_form'):
        return cands, ','.join(hints)
    if narrow({l for l in cands if index.records[l][6] in ('GENERAL', '')}, 'general_entity'):
        return cands, ','.join(hints)
    if narrow({l for l in cands if index.records[l][7] and index.records[l][7] == index.records[l][8]},
              'head_office'):
        return cands, ','.join(hints)
    if countries and narrow({l for l in cands if index.records[l][8] in countries}, 'jurisdiction'):
        return cands, ','.join(hints)
    # ANNULLED/DUPLICATE are excluded at load time; this drops LAPSED, RETIRED, MERGED.
    if narrow({l for l in cands if index.records[l][4] == 'ISSUED'}, 'issued_only'):
        return cands, ','.join(hints)
    if postcodes:
        if narrow({l for l in cands if index.records[l][5] and index.records[l][5] in postcodes}, 'postcode'):
            return cands, ','.join(hints)
    if cities:
        if narrow({l for l in cands if index.records[l][2] in cities}, 'city'):
            return cands, ','.join(hints)
    return cands, ','.join(hints)


def match_org(org, index: GleifIndex, whois: dict = None):
    """Return (lei, method, matched_name, hint) or (None, 'ambiguous'|'unmatched', cands, '').

    ``whois`` is the registry record from whois_enrich (name, country, city,
    postcode), if any. Its name is tried after the IYP and PeeringDB names, its
    country is used as an additional block when IYP has none matching, and city /
    postal code serve as tie-breakers.
    """
    whois = whois or {}
    norm = normalize(org.name)
    core, form = split_legal_form(norm)
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
    addresses = [(k, 'pdb_address') for k in (getattr(org, 'addresses', []) or [])]
    if whois.get('street'):
        wk = address_key(whois.get('country'), whois.get('postcode'), whois['street'])
        if wk and wk not in {a[0] for a in addresses}:
            addresses.append((wk, 'whois_address'))
    # Query names: the IYP name first, then PeeringDB name_long / aka, then whois.
    raw_norm = normalize(org.name, keep_paren=True)
    query_names = [(norm, core, '', legal_form_class(form), raw_norm)]
    for alt in getattr(org, 'alt_names', []) or []:
        an = normalize(alt)
        if an and an != norm:
            ac, af = split_legal_form(an)
            query_names.append((an, ac, 'alt_name', legal_form_class(af), normalize(alt, keep_paren=True)))
    if whois.get('name'):
        wn = normalize(whois['name'])
        if wn and wn not in {q[0] for q in query_names}:
            wc, wf = split_legal_form(wn)
            query_names.append((wn, wc, 'whois_name', legal_form_class(wf),
                                normalize(whois['name'], keep_paren=True)))

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
                cands, hint = _prefer(cands, index, cities, postcodes, qn[3], tuple(countries), qn[4])
                hint = ','.join(h for h in (src, csrc, hint) if h)
                if len(cands) == 1:
                    lei = next(iter(cands))
                    return lei, method, key, hint
                return None, 'ambiguous', sorted(cands), method

    # AS names: weaker evidence than the organization's own names, and an
    # organization can manage networks named after different entities, so they are
    # tried only after the tiers above, all variants are pooled, and a conflict
    # between two AS names is reported as ambiguous instead of picked arbitrarily.
    as_queries = dict()
    for raw in getattr(org, 'as_names', []) or []:
        for variant in as_name_variants(raw):
            n = normalize(variant)
            if not plausible_entity_name(n) or n in {q[0] for q in query_names} or n in as_queries:
                continue
            c, f = split_legal_form(n)
            as_queries[n] = (c, legal_form_class(f), normalize(variant, keep_paren=True))
    if as_queries:
        as_tiers = [
            ('as_name_exact', (index.exact, index.other), 0),
            ('as_name_core', (index.core_legal, index.core_other), 1),
        ]
        for method, idxs, which in as_tiers:
            by_variant = dict()
            for n, (c, fc, rn) in as_queries.items():
                key = n if which == 0 else c
                if not key:
                    continue
                found = set()
                for idx in idxs:
                    for countries, _csrc in country_sets:
                        for cc in countries:
                            found |= idx.get((cc, key), set())
                if found:
                    by_variant[n] = found
            if not by_variant:
                continue
            cands = set().union(*by_variant.values())
            # A single variant matched: its legal form and parenthetical are usable.
            fc, rn = ('', '')
            if len(by_variant) == 1:
                only = next(iter(by_variant))
                _c, fc, rn = as_queries[only]
            cands, hint = _prefer(cands, index, cities, postcodes, fc,
                                  tuple(iyp_countries + whois_countries), rn)
            hint = ','.join(h for h in ('as_name', hint) if h)
            if len(cands) == 1:
                return next(iter(cands)), method, '; '.join(sorted(by_variant)), hint
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
                best_leis, hint = _prefer(best_leis, index, cities, postcodes, legal_form_class(form),
                                          tuple(countries), raw_norm)
                if len(best_leis) == 1 and best_score - runner_up >= FUZZY_MARGIN:
                    lei = next(iter(best_leis))
                    if whois_countries and not iyp_countries:
                        hint = ','.join(h for h in ('whois_country', hint) if h)
                    return lei, 'fuzzy', hits[0][0], f'score={best_score:.0f}{"," + hint if hint else ""}'
                return None, 'ambiguous', sorted(best_leis), 'fuzzy'

    # Address tier: GLEIF HQ address at the same (country, postcode, street number) as
    # PeeringDB / the registry, plus name evidence. This mostly finds the parent or an
    # affiliate registered at the same headquarters, hence the low confidence.
    if addresses:
        core_tokens = distinctive_tokens(core)
        best = []
        for key, src in addresses:
            for lei in index.address.get(key, ()):
                cand_core = core_name(normalize(index.records[lei][0]))
                score = fuzz.token_set_ratio(core, cand_core)
                shared = core_tokens & distinctive_tokens(cand_core)
                if score >= ADDRESS_NAME_MIN_RATIO or shared:
                    best.append((score, lei, src))
        if best:
            top = max(b[0] for b in best)
            leis = {b[1] for b in best if b[0] >= top - FUZZY_MARGIN}
            src = next(b[2] for b in best if b[1] in leis)
            leis, hint = _prefer(leis, index, cities, postcodes, legal_form_class(form), tuple(countries),
                                 raw_norm)
            hint = ','.join(h for h in (src, f'score={top:.0f}', hint) if h)
            if len(leis) == 1:
                lei = next(iter(leis))
                return lei, 'address_name', core_name(normalize(index.records[lei][0])), hint
            return None, 'ambiguous', sorted(leis), 'address_name'

    # No country in IYP: only accept a globally unique exact legal-name match.
    if not countries:
        cands = index.global_exact.get(norm, set())
        cands, hint = _prefer(cands, index, cities, postcodes, legal_form_class(form), (), raw_norm)
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
    legal_name, countries, city, entity_status, reg_status, _pc, category, *_ = index.records[lei]
    return {
        'iyp_org_name': name,
        'lei': lei,
        'lei_legal_name': legal_name,
        'lei_countries': ';'.join(countries),
        'lei_address': index.records[lei][10],
        'entity_status': entity_status,
        'entity_category': category,
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
                # Same-looking names are distinct LEI records; show what tells them apart.
                'candidate_names': ' | '.join(
                    f'{index.records[l][0]} [{l}, {index.records[l][7] or "?"}, {index.records[l][6] or "?"}, '
                    f'{index.records[l][2] or "?"}, {index.records[l][3]}]' for l in extra),
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
        as_name = mapping[mapping.match_method.str.startswith('as_name')]
        if not as_name.empty:
            lines += ['', '## Matches from AS names (weakest evidence, review these)', '',
                      f"{len(as_name)} organization(s) matched only through a name attached to one of their ASes. "
                      'The AS name matched is in the `matched_name` column of the mapping.', '',
                      '| organization | ASes | AS name used | legal name | method |', '|---|---|---|---|---|']
            for r in as_name.sort_values('iyp_n_as', ascending=False).head(25).itertuples():
                lines.append(f'| {r.iyp_org_name} | {r.iyp_n_as} | {r.matched_name} | '
                             f'{r.lei_legal_name} | {r.match_method} |')
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
    p.add_argument('--rdap-budget', type=int, default=int(os.environ.get('RDAP_BUDGET', 6000)),
                   help='Maximum number of new RDAP requests per run (~45-60 minutes at 6000)')
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
    index = load_gleif(zip_path, os.path.join(args.out_dir, 'learned_legal_forms.json'))

    logging.info('Matching...')
    mapping, ambiguous, stats = run_matching(orgs, index)
    logging.info(f'Matching stats: {dict(stats)}')

    whois = {}
    if not args.no_whois:
        # Every organization not matched at >= 0.9 is worth a lookup because the
        # registered address feeds the address tier. First in line are those where the
        # registry adds a name or a country too: CAIDA's synthesized @aut-/@family-
        # organizations, organizations without a country, and ambiguous ones.
        good = set(mapping[mapping.confidence >= 0.9].iyp_org_name) if not mapping.empty else set()
        amb = set(ambiguous.iyp_org_name) if not ambiguous.empty else set()
        candidates, first = set(), set()
        for org in orgs.itertuples(index=False):
            if org.name in good:
                continue
            candidates.add(org.name)
            synthesized = any(str(c).startswith('@') for c in org.caida_ids)
            if synthesized or not org.countries or org.name in amb:
                first.add(org.name)
        logging.info(f'RDAP: {len(candidates)} organizations eligible for lookup, {len(first)} prioritized')
        whois = whois_enrich.enrich(orgs, args.rdap_cache, args.rdap_budget, first, only=candidates)
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
