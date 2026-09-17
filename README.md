# IYP ↔ GLEIF matchmaking ❤️

Curated mapping between [Internet Yellow Pages](https://iyp.iijlab.net) `Organization`
nodes and [GLEIF](https://www.gleif.org) Legal Entity Identifiers (LEI).

The mapping is regenerated weekly by a GitHub Action and committed to `data/`. It is
meant to be consumed by an IYP crawler as a dataset in its own right, in the same way IYP
imports CAIDA's heuristic AS-to-organization mapping: IYP does not reconcile sources at
import time, so the reconciliation has to live in a reviewable, versioned artifact like
this one.

## Outputs (`data/`)

| file | content |
|---|---|
| `iyp_gleif_mapping.csv` | one row per matched organization: IYP name, LEI, GLEIF legal name, `match_method`, `confidence`, PeeringDB/CAIDA org IDs, countries, number of managed ASes |
| `ambiguous.csv` | organizations with several equally good LEI candidates. **Review these first**, sorted by `iyp_n_as`, and resolve them in `overrides/`. |
| `unmatched.csv` | organizations with no candidate |
| `report.md` | summary statistics, also shown in the Action's job summary |
| `metadata.json` | run metadata (GLEIF publish date, counts) |

`cache/rdap_cache.json` (not committed, kept in the Actions cache) holds registry
lookups, see below.

## How matching works

Organizations in IYP are identified by name only, so the matcher pulls in everything
IYP knows around the node: countries (from PeeringDB, CAIDA as2org, ...), PeeringDB and
CAIDA org IDs, websites, and the number of ASes managed. IYP's `peeringdb.org` crawler
stores the raw PeeringDB org object on the `EXTERNAL_ID` relationship, so PeeringDB
`city`, `aka` and `name_long` are read from IYP as well; the two alternative names are
used as additional query names and the city as a tie-breaker. Everything comes from a
single Cypher query against IYP; no other API is needed. GLEIF records are indexed by
normalized name, **blocked by country** (union of legal jurisdiction, legal address
country and HQ country). Names are normalized (casefold, diacritics, punctuation) and a
"core" form additionally strips legal-form suffixes (`Inc`, `GmbH`, `株式会社`, ...).
GLEIF's *other entity names* (trading names, alternative-language and transliterated
names) are indexed too; network operators usually appear under a trading name.

### Registry (whois) enrichment

CAIDA as2org IDs stored in IYP (`CaidaOrgID`) are whois handles suffixed with their
registry, e.g. `LPL-141-ARIN`, `ORG-DTAG1-RIPE`, `@aut-2497-JPNIC`. `whois_enrich.py`
uses them for one RDAP lookup per organization: `{rir}/entity/{handle}` for real org
handles, `rdap.org/autnum/{asn}` for `@aut-` handles and for organizations without a
usable handle (first managed ASN). The registrant's name, country, city and postal code
are then fed into the matcher:

- the registered name is an extra query name (`hint` = `whois_name`). This mostly helps
  `@aut-` organizations, whose IYP name is only an as-name/descr while the aut-num now
  references a proper organisation object;
- the registered country is used as a block when IYP has no country, or no candidate in
  its countries (`hint` = `whois_country`);
- city and postal code break ties between same-name candidates (`hint` = `city`,
  `postcode`). Postal code also uses PeeringDB data when available.

Lookups run *after* a first matching pass, so the request budget (`--rdap-budget`,
default 3000 per run, `RDAP_BUDGET` in the Action) is spent on organizations that are
unmatched, ambiguous or matched below 0.9, largest first. Results (including 404s) are
cached for 180 days, so coverage grows run after run. Requests are throttled per host;
a 429 or 5xx disables that host for the rest of the run. Bulk whois dumps are not an
option here: RIPE and APNIC dummify organisation objects in their public dumps, and
ARIN/LACNIC bulk access requires an agreement.

Set expectations accordingly: for ARIN handles the registered name is what CAIDA already
reports, so the gain there is country/city/postcode; the name gain is for `@aut-` and NIR
(JPNIC, KRNIC, …) organizations. Large unmatched entities such as government agencies
simply have no LEI, whatever whois says.

### Tiers

Matching runs in tiers and stops at the first tier that yields candidates:

| `match_method` | confidence | rule |
|---|---|---|
| `manual` | 1.0 | from `overrides/manual_overrides.csv` |
| `legal_exact` | 1.0 | normalized name (or PeeringDB `name_long`/`aka`, flagged `alt_name` in `hint`) == GLEIF legal name, same country |
| `other_name_exact` | 0.95 | normalized name == one of GLEIF's other names, same country |
| `legal_core` | 0.9 | legal-form-stripped names equal, same country |
| `other_name_core` | 0.85 | same, against other names |
| `fuzzy` | 0.7 | `token_sort_ratio >= 92` within (country, first token) block, unique best with margin |
| `legal_exact_nocountry` | 0.5 | org has no country in IYP; globally unique exact legal name |

When a tier returns several LEIs, ties are broken by entity status (`ACTIVE` only), then
by postal code and city (from PeeringDB or the registry) vs. GLEIF's legal address. If that still leaves more than one
candidate the organization goes to `ambiguous.csv` rather than being guessed.
`ANNULLED` and `DUPLICATE` LEI registrations are never matched.

Precision is favoured over recall on purpose. Consumers should filter on `confidence`;
`>= 0.9` is a reasonable default for IYP.

## Manual overrides

`overrides/manual_overrides.csv` is applied last:

```csv
iyp_org_name,lei,action
Acme Networks,529900ABCDEFGHIJKL12,accept
Cogent Communication Group Inc,LEI...,reject
```

`accept` forces a match (and replaces the automatic one); `reject` drops a specific
match, or every match for the org if `lei` is empty. Pushing to `overrides/` re-runs the
Action.

## Running locally

Requires [uv](https://docs.astral.sh/uv/). Dependencies are pinned in `uv.lock`.

```sh
uv sync
uv run match-gleif                     # queries public IYP, downloads GLEIF (~300 MB zip)
uv run match-gleif --gleif-zip cache/gleif-lei2-YYYY-MM-DD.csv.zip   # reuse a download
uv run match-gleif --iyp-csv orgs.csv --gleif-zip test.zip --no-whois  # offline test
uv run match-gleif --rdap-budget 0             # use cached registry lookups only
uv lock --upgrade                      # refresh pinned dependencies
```

Environment: `IYP_BOLT_URI`, `IYP_USER`, `IYP_PASSWORD` (default: public IYP instance),
`RDAP_BUDGET` (default 3000).

The Action reads the same variables from repository variables/secrets, so the workflow
runs unchanged against a private IYP instance.

## Known limitations / next steps

- LEI coverage is driven by financial reporting obligations. Large operators, banks,
  cloud providers and EU/UK entities match well; many small ISPs have no LEI at all, so a
  low overall match rate with high AS coverage is the expected outcome.
- GLEIF has no website field, so IYP websites are carried through for review but not
  used for matching. Matching the website's registrable domain against GLEIF names is a
  possible extra signal.
- PeeringDB `zipcode` is also available on the `EXTERNAL_ID` relationship and could feed
  the postal-code tie-breaker like the registry postal code does.
- NIR handles (JPNIC, KRNIC, TWNIC, …) are only resolved through APNIC's autnum RDAP,
  which may return a stub; querying the NIRs' own RDAP servers would improve this.
- CAIDA as2org names are often truncated or all-caps whois names; a per-source
  normalization could help.
- The next stage is an IYP crawler that imports `iyp_gleif_mapping.csv` as
  `(:Organization)-[:EXTERNAL_ID {match_method, confidence}]->(:LEI)` plus GLEIF Level 1
  attributes and Level 2 parent relationships for the matched LEIs.

## Data terms

GLEIF golden copy files are provided free of charge under the
[LEI Data Terms of Use](https://www.gleif.org/en/meta/lei-data-terms-of-use).
