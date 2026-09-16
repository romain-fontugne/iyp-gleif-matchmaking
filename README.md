# IYP ↔ GLEIF organization mapping

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

When a tier returns several LEIs, ties are broken by entity status (`ACTIVE` only) and
then by PeeringDB city vs. GLEIF legal-address city. If that still leaves more than one
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
uv run match-gleif --iyp-csv orgs.csv --gleif-zip test.zip  # offline test
uv lock --upgrade                      # refresh pinned dependencies
```

Environment: `IYP_BOLT_URI`, `IYP_USER`, `IYP_PASSWORD` (default: public IYP instance).

The Action reads the same variables from repository variables/secrets, so the workflow
runs unchanged against a private IYP instance.

## Known limitations / next steps

- LEI coverage is driven by financial reporting obligations. Large operators, banks,
  cloud providers and EU/UK entities match well; many small ISPs have no LEI at all, so a
  low overall match rate with high AS coverage is the expected outcome.
- GLEIF has no website field, so IYP websites are carried through for review but not
  used for matching. Matching the website's registrable domain against GLEIF names is a
  possible extra signal.
- PeeringDB `zipcode` is also available on the `EXTERNAL_ID` relationship and could serve
  as a second tie-breaker against GLEIF's legal-address postal code.
- CAIDA as2org names are often truncated or all-caps whois names; a per-source
  normalization could help.
- The next stage is an IYP crawler that imports `iyp_gleif_mapping.csv` as
  `(:Organization)-[:EXTERNAL_ID {match_method, confidence}]->(:LEI)` plus GLEIF Level 1
  attributes and Level 2 parent relationships for the matched LEIs.

## Data terms

GLEIF golden copy files are provided free of charge under the
[LEI Data Terms of Use](https://www.gleif.org/en/meta/lei-data-terms-of-use).
