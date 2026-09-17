"""Enrich IYP organizations with registry (whois) data via RDAP.

CAIDA as2org organization IDs stored in IYP (CaidaOrgID) are whois handles suffixed
with the registry they came from, e.g. ``LPL-141-ARIN``, ``ORG-DTAG1-RIPE`` or
``@aut-2497-JPNIC``. The ``@aut-`` and ``@family-`` prefixes mark organizations CAIDA
synthesized from aut-num objects that have no org object; their IYP name is then only
an as-name/descr, and the registry may point to a proper organisation object today.

For each organization we perform at most one RDAP lookup:

  1. a real org handle           -> {rir}/entity/{handle}
  2. an ``@aut-N-…`` handle      -> https://rdap.org/autnum/N   (bootstrapped to the RIR)
  3. otherwise the first ASN      -> https://rdap.org/autnum/{asn}

and keep the registrant's name, country, city and postal code. Results (including
404s) are cached on disk so the weekly run only spends its request budget on
organizations that have not been looked up yet.

RIR bulk dumps are not an alternative: RIPE (and APNIC, which runs the same software)
dummify organisation objects in the public FTP dumps, and ARIN/LACNIC bulk whois
requires an agreement. RDAP is the only public, structured source for this.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import pycountry
import requests

RDAP_BASE = {
    'ARIN': 'https://rdap.arin.net/registry',
    'RIPE': 'https://rdap.db.ripe.net',
    'APNIC': 'https://rdap.apnic.net',
    'LACNIC': 'https://rdap.lacnic.net/rdap',
    'AFRINIC': 'https://rdap.afrinic.net/rdap',
}
RDAP_BOOTSTRAP = 'https://rdap.org'
# Minimum seconds between requests to the same host. LACNIC is strict.
HOST_INTERVAL = {
    'rdap.arin.net': 0.3,
    'rdap.db.ripe.net': 0.15,
    'rdap.apnic.net': 0.3,
    'rdap.lacnic.net': 1.5,
    'rdap.afrinic.net': 0.5,
    'rdap.org': 0.3,
}
DEFAULT_INTERVAL = 0.5
CACHE_TTL = timedelta(days=180)
NEGATIVE_TTL = timedelta(days=60)

_HANDLE_RE = re.compile(r'^(?P<handle>.+)-(?P<source>[A-Z][A-Z0-9.]+)$')
_AUT_RE = re.compile(r'^@aut-(?P<asn>\d+)$')


def parse_caida_id(caida_id: str):
    """'ORG-DTAG1-RIPE' -> ('ORG-DTAG1', 'RIPE'); '@aut-2497-JPNIC' -> ('@aut-2497', 'JPNIC')."""
    m = _HANDLE_RE.match(caida_id or '')
    if not m:
        return None, None
    return m.group('handle'), m.group('source')


# Registries whose org handles embed the registry suffix themselves (ORG-XY1-RIPE,
# ORG-XY1-AP, XY-LACNIC, ORG-XY1-AFRINIC). CAIDA does not repeat the suffix for
# these, whereas for ARIN it appends "-ARIN" to the bare handle (LPL-141-ARIN).
_SUFFIXED_HANDLES = {'RIPE', 'APNIC', 'AP', 'LACNIC', 'AFRINIC'}
_SOURCE_ALIAS = {'AP': 'APNIC'}


def lookup_urls(caida_ids: list, asns: list) -> list:
    """Ordered RDAP URLs to try for an organization (first 200 wins)."""
    urls = []
    autnums = []
    for cid in caida_ids:
        handle, source = parse_caida_id(cid)
        if not handle:
            continue
        aut = _AUT_RE.match(handle)
        if aut:
            autnums.append(int(aut.group('asn')))
            continue
        if handle.startswith('@'):
            continue  # @family-…: no registry object
        rir = _SOURCE_ALIAS.get(source, source)
        if rir not in RDAP_BASE:
            continue  # NIR handles (JPNIC, KRNIC, …) are not resolvable via entity lookup
        base = RDAP_BASE[rir]
        if source in _SUFFIXED_HANDLES:
            # e.g. ORG-DTAG1-RIPE: the full ID is the handle. APNIC handles end in
            # "-AP"; CAIDA may or may not have appended "-APNIC" on top of that.
            full = cid[:-len('-APNIC')] if cid.endswith('-AP-APNIC') else cid
            candidates = [full, handle] if handle != full else [full]
        else:
            candidates = [handle]
        urls.extend(f'{base}/entity/{h}' for h in candidates)
    for asn in autnums + [int(a) for a in asns if str(a).isdigit()][:1]:
        urls.append(f'{RDAP_BOOTSTRAP}/autnum/{asn}')
    # De-duplicate, keep order.
    seen = set()
    return [u for u in urls if not (u in seen or seen.add(u))]


# --------------------------------------------------------------------------- #
# jCard parsing
# --------------------------------------------------------------------------- #

def _country_code(value: str) -> str:
    if not value:
        return ''
    v = value.strip()
    if len(v) == 2 and v.isalpha():
        return v.upper()
    try:
        return pycountry.countries.lookup(v).alpha_2
    except LookupError:
        return ''


def parse_vcard(vcard_array) -> dict:
    out = {}
    if not vcard_array or len(vcard_array) < 2:
        return out
    for item in vcard_array[1]:
        if len(item) < 4:
            continue
        key, params, value = item[0], item[1] or {}, item[3]
        if key == 'fn' and value:
            out['name'] = str(value).strip()
        elif key == 'adr':
            # Structured: [pobox, ext, street, locality, region, postcode, country]
            if isinstance(value, list) and len(value) >= 7 and any(value):
                out['city'] = out.get('city') or str(value[3] or '').strip()
                out['postcode'] = out.get('postcode') or str(value[5] or '').strip()
                out['country'] = out.get('country') or _country_code(str(value[6] or ''))
            label = params.get('label')
            if label:
                lines = [l.strip() for l in str(label).splitlines() if l.strip()]
                out.setdefault('address', ' | '.join(lines))
                # Registries that only give a free-text label (RIPE) often end with
                # the country. Take it only if it parses unambiguously.
                if not out.get('country') and lines:
                    out['country'] = _country_code(lines[-1])
                    if not out['country'] and len(lines) > 1:
                        out['country'] = _country_code(lines[-2])
    return out


def _iter_entities(obj):
    for ent in obj.get('entities', []) or []:
        yield ent
        yield from _iter_entities(ent)


def parse_rdap(doc: dict) -> dict:
    """Extract registrant organisation info from an entity or autnum response."""
    info = {}
    if doc.get('objectClassName') == 'entity':
        info = parse_vcard(doc.get('vcardArray'))
        info['handle'] = doc.get('handle', '')
    else:
        registrant = None
        for ent in _iter_entities(doc):
            roles = set(ent.get('roles', []) or [])
            if 'registrant' in roles and ent.get('vcardArray'):
                registrant = ent
                break
        if registrant is None:
            # Fall back to the first entity that looks like an organisation.
            for ent in _iter_entities(doc):
                vc = parse_vcard(ent.get('vcardArray'))
                kinds = [i[3] for i in (ent.get('vcardArray') or [None, []])[1] if i and i[0] == 'kind']
                if vc.get('name') and (not kinds or kinds[0] in ('org', 'group')):
                    registrant = ent
                    break
        if registrant is not None:
            info = parse_vcard(registrant.get('vcardArray'))
            info['handle'] = registrant.get('handle', '')
        if not info.get('country') and doc.get('country'):
            info['country'] = _country_code(doc['country'])
        info['autnum_name'] = doc.get('name', '')
    return {k: v for k, v in info.items() if v}


# --------------------------------------------------------------------------- #
# Cache + fetch
# --------------------------------------------------------------------------- #

class RdapCache:
    def __init__(self, path: str):
        self.path = path
        self.data = {}
        if os.path.exists(path):
            with open(path) as f:
                self.data = json.load(f)
            logging.info(f'Loaded {len(self.data)} cached RDAP lookups from {path}')

    def fresh(self, url: str) -> bool:
        e = self.data.get(url)
        if not e:
            return False
        ts = datetime.fromisoformat(e['fetched'])
        ttl = CACHE_TTL if e.get('status') == 200 else NEGATIVE_TTL
        return datetime.now(tz=timezone.utc) - ts < ttl

    def get(self, url: str) -> dict:
        return (self.data.get(url) or {}).get('info', {})

    def put(self, url: str, status: int, info: dict):
        self.data[url] = {'fetched': datetime.now(tz=timezone.utc).isoformat(), 'status': status, 'info': info}

    def save(self):
        os.makedirs(os.path.dirname(self.path) or '.', exist_ok=True)
        tmp = self.path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(self.data, f)
        os.replace(tmp, self.path)


class RdapClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/rdap+json, application/json',
                                     'User-Agent': 'iyp-gleif-matchmaking (+https://github.com/romain-fontugne/iyp-gleif-matchmaking)'})
        self.last_request = {}
        self.blocked_hosts = set()

    def _throttle(self, host: str):
        wait = HOST_INTERVAL.get(host, DEFAULT_INTERVAL) - (time.monotonic() - self.last_request.get(host, 0))
        if wait > 0:
            time.sleep(wait)
        self.last_request[host] = time.monotonic()

    def fetch(self, url: str):
        """Return (status, info). status 0 means the request was not performed."""
        host = urlsplit(url).hostname
        if host in self.blocked_hosts:
            return 0, {}
        self._throttle(host)
        try:
            r = self.session.get(url, timeout=30, allow_redirects=True)
        except requests.RequestException as e:
            logging.warning(f'RDAP request failed for {url}: {e}')
            return 0, {}
        final_host = urlsplit(r.url).hostname
        if final_host != host:
            self.last_request[final_host] = time.monotonic()
        if r.status_code == 429 or r.status_code >= 500:
            logging.warning(f'RDAP {r.status_code} from {final_host}; skipping this host for the rest of the run')
            self.blocked_hosts.add(final_host)
            if final_host != host:
                self.blocked_hosts.add(host)
            return 0, {}
        if r.status_code != 200:
            return r.status_code, {}
        try:
            return 200, parse_rdap(r.json())
        except ValueError:
            logging.warning(f'RDAP response for {url} is not JSON')
            return 0, {}


def enrich(orgs, cache_path: str, budget: int, priority_names: set = None, only: set = None) -> dict:
    """Look up organizations via RDAP. Returns {org_name: info}.

    ``orgs`` must have columns name, caida_ids, asns, n_as. Cached results are used
    for every organization; at most ``budget`` new HTTP requests are made, spent on
    ``priority_names`` first (typically organizations the matcher could not match),
    ordered by number of ASes. For each organization the candidate URLs are tried in
    order until one returns data; every attempt is cached, including 404s. If
    ``only`` is given, new lookups are restricted to those organizations (cached
    results are still used for everyone).
    """
    cache = RdapCache(cache_path)
    client = RdapClient()
    result = {}
    todo = []
    for org in orgs.itertuples(index=False):
        urls = lookup_urls(org.caida_ids, getattr(org, 'asns', []) or [])
        if not urls:
            continue
        pending = []
        for url in urls:
            if cache.fresh(url):
                info = cache.get(url)
                if info:
                    result[org.name] = info
                    pending = []
                    break
                continue  # cached negative: try the next candidate
            pending.append(url)
        if pending and (only is None or org.name in only):
            prio = 0 if priority_names is None or org.name in priority_names else 1
            todo.append((prio, -int(org.n_as), org.name, pending))
    todo.sort()
    logging.info(f'RDAP: {len(result)} organizations served from cache, {len(todo)} pending, budget {budget}')
    done = 0
    try:
        for _, _, name, urls in todo:
            if done >= budget:
                break
            for url in urls:
                if done >= budget:
                    break
                status, info = client.fetch(url)
                if status == 0:
                    continue
                done += 1
                cache.put(url, status, info)
                if info:
                    result[name] = info
                    break
                if done % 200 == 0:
                    cache.save()
                    logging.info(f'RDAP: {done} lookups done')
    finally:
        cache.save()
    logging.info(f'RDAP: performed {done} lookups; {len(result)} organizations enriched in total')
    return result
