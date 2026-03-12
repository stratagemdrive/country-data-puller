"""
Build a Base44-friendly JSON snapshot for multiple countries.

Output: public/countries_snapshot.json

Fields returned per country:
- Head of State (+ party)  [Wikidata current statement, no end date]
- Head of Government (+ party) [Wikidata current statement, no end date]
- Legislature body/bodies (filtered to "legislature" items)
- Party/group in charge of legislature body/bodies (best-effort via last legislative/general election winner; often missing)
- Executive party/leader (best-effort: HoG party -> fallback HoS party)
- World Bank governance snapshot (WGI percentile ranks; overall + components)
  - Pulls latest non-null values
  - Sticky behavior: if fetch fails and prior exists, keep prior values
  - Qualitative 5-tier labels alongside raw percentiles
- Political system type (Wikidata P122 labels)
- Next legislative election (date + type + exists?)  [IPU Parline primary, Wikidata fallback]
- Next executive election (date + type + exists?)    [Wikidata; IPU covers legislative only]
- Country metadata (capital, population, region, flag, currencies, languages) [REST Countries]

Data sources:
- Wikidata SPARQL
- World Bank Indicators API (Worldwide Governance Indicators - WGI)
- IPU Parline API (parliamentary election schedules + last election date)
- REST Countries API (country metadata)
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# ---------------------------- CONFIG ----------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
TIMEOUT = 25
MAX_RETRIES = 3
RETRY_SLEEP = 1.5

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WORLD_BANK_BASE = "https://api.worldbank.org/v2"
IPU_API_BASE    = "https://api.data.ipu.org/v1"
REST_COUNTRIES_BASE = "https://restcountries.com/v3.1"

# WGI percentile rank indicators (0..100)
WGI_PERCENTILE_INDICATORS: Dict[str, str] = {
    "voiceAccountability": "VA.PER.RNK",
    "politicalStability": "PV.PER.RNK",
    "governmentEffectiveness": "GE.PER.RNK",
    "regulatoryQuality": "RQ.PER.RNK",
    "ruleOfLaw": "RL.PER.RNK",
    "controlOfCorruption": "CC.PER.RNK",
}

# Human-readable label templates per dimension
WGI_LABEL_TEMPLATES: Dict[str, Dict[str, str]] = {
    "voiceAccountability": {
        "Very Low":  "Very low voice & accountability",
        "Low":       "Low voice & accountability",
        "Medium":    "Moderate voice & accountability",
        "High":      "High voice & accountability",
        "Very High": "Very high voice & accountability",
    },
    "politicalStability": {
        "Very Low":  "Very low political stability",
        "Low":       "Low political stability",
        "Medium":    "Moderate political stability",
        "High":      "High political stability",
        "Very High": "Very high political stability",
    },
    "governmentEffectiveness": {
        "Very Low":  "Very low government effectiveness",
        "Low":       "Low government effectiveness",
        "Medium":    "Moderate government effectiveness",
        "High":      "High government effectiveness",
        "Very High": "Very high government effectiveness",
    },
    "regulatoryQuality": {
        "Very Low":  "Very low regulatory quality",
        "Low":       "Low regulatory quality",
        "Medium":    "Moderate regulatory quality",
        "High":      "High regulatory quality",
        "Very High": "Very high regulatory quality",
    },
    "ruleOfLaw": {
        "Very Low":  "Very low rule of law",
        "Low":       "Low rule of law",
        "Medium":    "Moderate rule of law",
        "High":      "High rule of law",
        "Very High": "Very high rule of law",
    },
    "controlOfCorruption": {
        "Very Low":  "Very low control of corruption",
        "Low":       "Low control of corruption",
        "Medium":    "Moderate control of corruption",
        "High":      "High control of corruption",
        "Very High": "Very high control of corruption",
    },
}

WGI_OVERALL_LABELS: Dict[str, str] = {
    "Very Low":  "Very low governance overall",
    "Low":       "Low governance overall",
    "Medium":    "Moderate governance overall",
    "High":      "High governance overall",
    "Very High": "Very high governance overall",
}


# ---------------------------- COUNTRY LIST ----------------------------

COUNTRIES: List[Dict[str, str]] = [
    {"country": "Russia",       "iso2": "RU"},
    {"country": "India",        "iso2": "IN"},
    {"country": "Pakistan",     "iso2": "PK"},
    {"country": "China",        "iso2": "CN"},
    {"country": "United Kingdom","iso2": "GB"},
    {"country": "Germany",      "iso2": "DE"},
    {"country": "UAE",          "iso2": "AE"},
    {"country": "Saudi Arabia", "iso2": "SA"},
    {"country": "Israel",       "iso2": "IL"},
    {"country": "Palestine",    "iso2": "PS"},
    {"country": "Mexico",       "iso2": "MX"},
    {"country": "Brazil",       "iso2": "BR"},
    {"country": "Canada",       "iso2": "CA"},
    {"country": "Nigeria",      "iso2": "NG"},
    {"country": "Japan",        "iso2": "JP"},
    {"country": "Iran",         "iso2": "IR"},
    {"country": "Syria",        "iso2": "SY"},
    {"country": "France",       "iso2": "FR"},
    {"country": "Turkey",       "iso2": "TR"},
    {"country": "Venezuela",    "iso2": "VE"},
    {"country": "Vietnam",      "iso2": "VN"},
    {"country": "Taiwan",       "iso2": "TW"},
    {"country": "South Korea",  "iso2": "KR"},
    {"country": "North Korea",  "iso2": "KP"},
    {"country": "Indonesia",    "iso2": "ID"},
    {"country": "Myanmar",      "iso2": "MM"},
    {"country": "Armenia",      "iso2": "AM"},
    {"country": "Azerbaijan",   "iso2": "AZ"},
    {"country": "Morocco",      "iso2": "MA"},
    {"country": "Somalia",      "iso2": "SO"},
    {"country": "Yemen",        "iso2": "YE"},
    {"country": "Libya",        "iso2": "LY"},
    {"country": "Egypt",        "iso2": "EG"},
    {"country": "Algeria",      "iso2": "DZ"},
    {"country": "Argentina",    "iso2": "AR"},
    {"country": "Chile",        "iso2": "CL"},
    {"country": "Peru",         "iso2": "PE"},
    {"country": "Cuba",         "iso2": "CU"},
    {"country": "Colombia",     "iso2": "CO"},
    {"country": "Panama",       "iso2": "PA"},
    {"country": "El Salvador",  "iso2": "SV"},
    {"country": "Denmark",      "iso2": "DK"},
    {"country": "Sudan",        "iso2": "SD"},
    {"country": "Ukraine",      "iso2": "UA"},
]

# IPU uses ISO2 codes, but a few countries need remapping.
# Taiwan (TW) is not in IPU (not a UN member state).
IPU_ISO2_OVERRIDES: Dict[str, Optional[str]] = {
    "TW": None,   # Taiwan not in IPU — skip gracefully
}

# ---------------------------------------------------------------------------
# STATIC EXECUTIVE OVERRIDES
# Used when Wikidata data is known to be wrong or stale.
# Keys are ISO2. Each entry can override: hosName, hosParty, hogName, hogParty
# ---------------------------------------------------------------------------
STATIC_EXECUTIVE_OVERRIDES: Dict[str, Dict[str, Optional[str]]] = {
    # Putin's United Russia party is not on his Wikidata P102 statement
    "RU": {"hosParty": "United Russia", "hogParty": "United Russia"},
    # Tinubu: Wikidata still shows old "Action Congress of Nigeria" pre-merger party
    "NG": {"hosParty": "All Progressives Congress", "hogParty": "All Progressives Congress"},
    # Macron: Wikidata P102 shows old "Socialist Party" membership; correct party is Renaissance
    "FR": {"hosParty": "Renaissance", "hogParty": "Renaissance"},
    # Syria: transitional government — Ahmad al-Sharaa leads HTS-backed administration
    # Wikidata hasn't caught up since Assad fell in Dec 2024
    "SY": {
        "hosName": "Ahmad al-Sharaa",
        "hosParty": "Hayat Tahrir al-Sham (transitional)",
        "hogName": "Mohammad al-Bashir",
        "hogParty": "Hayat Tahrir al-Sham (transitional)",
    },
    # Venezuela: Maduro is actual HoS/HoG; Wikidata P35 currently resolves to Delcy Rodriguez
    "VE": {
        "hosName": "Nicolás Maduro",
        "hosParty": "United Socialist Party of Venezuela",
        "hogName": "Nicolás Maduro",
        "hogParty": "United Socialist Party of Venezuela",
    },
    # South Korea: constitutional crisis — Yoon impeached, Han Duck-soo acting president
    # Wikidata is picking up Lee Jae-myung (opposition leader) incorrectly
    "KR": {
        "hosName": "Han Duck-soo (acting)",
        "hosParty": "People Power Party",
        "hogName": "Han Duck-soo (acting)",
        "hogParty": "People Power Party",
    },
    # Iran: Wikidata P35 is resolving to Mojtaba Khamenei (son); correct HoS is Ali Khamenei
    "IR": {
        "hosName": "Ali Khamenei",
        "hosParty": "Association of Combatant Clergy",
    },
}

# ---------------------------------------------------------------------------
# DATA AVAILABILITY NOTES
# ---------------------------------------------------------------------------
DATA_AVAILABILITY_NOTES: Dict[str, Dict[str, str]] = {
    "TW": {
        "worldBankGovernance": (
            "Taiwan is not a UN member state and is not recognized by the World Bank as a "
            "sovereign country. The WB API does not include Taiwan in its governance indicators."
        ),
        "elections.legislative": (
            "Taiwan is not a member of the Inter-Parliamentary Union (IPU) and is therefore "
            "absent from the Parline parliamentary database."
        ),
    },
    "PS": {
        "worldBankGovernance": (
            "World Bank data for Palestine (West Bank and Gaza) is limited and may be "
            "incomplete due to the territory's political status."
        ),
    },
    "KP": {
        "worldBankGovernance": (
            "North Korea data in World Bank governance indicators is based on limited "
            "external assessments due to restricted access."
        ),
        "elections.legislative": (
            "North Korea holds nominal elections with a single-party slate; "
            "IPU Parline may not track these as competitive legislative elections."
        ),
    },
    "SY": {
        "executive": (
            "Syria's transitional government formed after Assad's fall in December 2024 "
            "is not yet fully reflected in Wikidata. Executive data is from static overrides."
        ),
    },
    "SO": {
        "worldBankGovernance": (
            "Somalia's governance data is based on limited external assessments "
            "due to ongoing conflict and restricted institutional access."
        ),
    },
    "YE": {
        "worldBankGovernance": (
            "Yemen's governance data reflects the pre-conflict institutional baseline; "
            "current effective governance is severely disrupted by civil war."
        ),
    },
    "LY": {
        "worldBankGovernance": (
            "Libya has parallel governing authorities; governance data reflects "
            "the internationally recognized government's institutional capacity."
        ),
    },
}


# ---------------------------- HELPERS ----------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_SLEEP * attempt)

def req_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    label: str = "",
) -> Optional[Any]:
    """
    GET a URL and return parsed JSON, or None on failure.
    Logs unexpected HTTP status codes to aid diagnosis.
    """
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=h, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                print(f"    [req_json] {label or url} → HTTP {r.status_code} (not found / bad request)")
                return None
            # Unexpected status — log and retry
            print(
                f"    [req_json] {label or url} → unexpected HTTP {r.status_code} "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )
        except requests.RequestException as exc:
            print(f"    [req_json] {label or url} → request error (attempt {attempt}/{MAX_RETRIES}): {exc}")
        _sleep_backoff(attempt)
    print(f"    [req_json] {label or url} → all {MAX_RETRIES} attempts failed, returning None")
    return None

def safe_get(d: Any, *keys: str, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def load_previous_snapshot(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        countries = data.get("countries", [])
        by_iso2: Dict[str, Any] = {}
        for c in countries:
            iso2 = c.get("iso2")
            if iso2:
                by_iso2[iso2] = c
        return by_iso2
    except Exception:
        return {}


# ---------------------------- QUALITATIVE LABELS ----------------------------

def percentile_to_tier(percentile: Optional[float]) -> Optional[str]:
    if percentile is None:
        return None
    if percentile < 20:
        return "Very Low"
    if percentile < 40:
        return "Low"
    if percentile < 60:
        return "Medium"
    if percentile < 80:
        return "High"
    return "Very High"

def percentile_to_label(percentile: Optional[float], dimension: str) -> Optional[str]:
    tier = percentile_to_tier(percentile)
    if tier is None:
        return None
    templates = WGI_LABEL_TEMPLATES.get(dimension, {})
    return templates.get(tier, tier)

def overall_percentile_to_label(percentile: Optional[float]) -> Optional[str]:
    tier = percentile_to_tier(percentile)
    if tier is None:
        return None
    return WGI_OVERALL_LABELS.get(tier, tier)


# ---------------------------- WIKIDATA ----------------------------

def wikidata_sparql(query: str) -> Optional[dict]:
    return req_json(
        WIKIDATA_SPARQL,
        params={"format": "json", "query": query},
        headers={"Accept": "application/sparql-results+json"},
        label="Wikidata SPARQL",
    )

def _wd_val(b: dict, key: str) -> Optional[str]:
    v = b.get(key)
    if not v:
        return None
    return v.get("value")

def get_wikidata_country_qid_by_iso2(iso2: str) -> Optional[str]:
    q = f"""
    SELECT ?country WHERE {{
      ?country wdt:P297 "{iso2}" .
    }} LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return None
    uri = _wd_val(bindings[0], "country")
    if not uri:
        return None
    return uri.rsplit("/", 1)[-1]

def get_political_system_labels(country_qid: str) -> List[str]:
    q = f"""
    SELECT ?polsysLabel WHERE {{
      wd:{country_qid} wdt:P122 ?polsys .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 20
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    out: List[str] = []
    for b in bindings:
        lab = _wd_val(b, "polsysLabel")
        if lab and lab not in out:
            out.append(lab)
    return out

def get_current_officeholder(country_qid: str, prop: str) -> Dict[str, Optional[str]]:
    q = f"""
    SELECT ?personLabel ?partyLabel ?start WHERE {{
      wd:{country_qid} p:{prop} ?stmt .
      ?stmt ps:{prop} ?person .
      FILTER NOT EXISTS {{ ?stmt pq:P582 ?end . }}
      OPTIONAL {{ ?stmt pq:P580 ?start . }}
      OPTIONAL {{ ?person wdt:P102 ?party . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?start)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {"name": None, "party": None}
    b = bindings[0]
    return {
        "name": _wd_val(b, "personLabel"),
        "party": _wd_val(b, "partyLabel"),
    }

def get_legislature_bodies(country_qid: str) -> List[str]:
    q = f"""
    SELECT ?legLabel WHERE {{
      wd:{country_qid} wdt:P194 ?leg .
      ?leg wdt:P31/wdt:P279* wd:Q11204 .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    out: List[str] = []
    for b in bindings:
        lab = _wd_val(b, "legLabel")
        if lab and lab not in out:
            out.append(lab)
    return out

def get_government_snapshot(country_qid: str) -> Dict[str, Any]:
    hos = get_current_officeholder(country_qid, "P35")
    hog = get_current_officeholder(country_qid, "P6")
    legislatures = get_legislature_bodies(country_qid)
    return {
        "headOfState": hos,
        "headOfGovernment": hog,
        "legislatureBodies": legislatures,
        "executiveController": {
            "leader": (hog.get("name") or hos.get("name")),
            "partyOrGroup": (hog.get("party") or hos.get("party") or "unknown"),
            "method": "hog_party_else_hos_party",
        },
    }


# ---------------------------- WIKIDATA ELECTIONS ----------------------------

def _today_yyyymmdd() -> str:
    return now_utc().strftime("%Y-%m-%dT00:00:00Z")

def get_next_election_upcoming_wikidata(country_qid: str, kind: str) -> Dict[str, Any]:
    """
    Wikidata upcoming election lookup.

    KEY FIX: Uses UNION of wdt:P1001 (applies to jurisdiction) and wdt:P17 (country)
    because most election items in Wikidata use P17, not P1001.
    Also includes Q40231 (election) as a broad type fallback alongside specific types.
    """
    today = _today_yyyymmdd()

    if kind == "executive":
        type_filter = "VALUES ?type { wd:Q159821 wd:Q152203 wd:Q40231 }"
    else:
        type_filter = "VALUES ?type { wd:Q1079032 wd:Q104203 wd:Q152203 wd:Q40231 }"

    q = f"""
    SELECT ?eLabel ?date ?typeLabel WHERE {{
      {{
        ?e wdt:P1001 wd:{country_qid} .
      }} UNION {{
        ?e wdt:P17 wd:{country_qid} .
      }}
      ?e wdt:P585 ?date .
      FILTER(?date >= "{today}"^^xsd:dateTime)
      ?e wdt:P31 ?type .
      {type_filter}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY ASC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])

    if not bindings:
        return {
            "exists": "unknown",
            "nextDate": None,
            "electionType": None,
            "method": "wikidata_upcoming",
            "notes": "No upcoming election item found in Wikidata (common gap).",
        }

    b = bindings[0]
    return {
        "exists": True,
        "nextDate": _wd_val(b, "date"),
        "electionType": _wd_val(b, "typeLabel"),
        "method": "wikidata_upcoming",
        "notes": "From Wikidata upcoming election items (P1001/P17 UNION + future date).",
    }

def get_last_legislative_election_winner(country_qid: str) -> Dict[str, Any]:
    """
    KEY FIX: Uses UNION of wdt:P1001 and wdt:P17, plus Q40231 broad fallback.
    """
    today = _today_yyyymmdd()
    q = f"""
    SELECT ?eLabel ?date ?winnerLabel WHERE {{
      {{
        ?e wdt:P1001 wd:{country_qid} .
      }} UNION {{
        ?e wdt:P17 wd:{country_qid} .
      }}
      ?e wdt:P585 ?date .
      FILTER(?date <= "{today}"^^xsd:dateTime)
      ?e wdt:P31 ?type .
      VALUES ?type {{ wd:Q152203 wd:Q1079032 wd:Q104203 wd:Q40231 }}
      OPTIONAL {{ ?e wdt:P1346 ?winner . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY DESC(?date)
    LIMIT 1
    """
    data = wikidata_sparql(q)
    bindings = safe_get(data, "results", "bindings", default=[])
    if not bindings:
        return {
            "winner": "unknown",
            "method": "wikidata_last_leg_election_winner",
            "notes": "No prior legislative election item found in Wikidata.",
        }

    b = bindings[0]
    return {
        "winner": _wd_val(b, "winnerLabel") or "unknown",
        "method": "wikidata_last_leg_election_winner",
        "notes": "Approximate: last national legislative election winner (Wikidata P1346). Coalitions/seat majorities may differ.",
        "basis": {
            "electionName": _wd_val(b, "eLabel"),
            "electionDate": _wd_val(b, "date"),
        },
    }


# ---------------------------- IPU PARLINE ----------------------------

# One-time cache: fetched once at startup, reused for all countries.
_ipu_chamber_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None

def _load_ipu_cache() -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch chamber data from IPU Parline using the per-chamber endpoint:
      GET /v1/chambers/{ISO2}

    Logs the raw response shape for the FIRST successful fetch to aid
    diagnosis when the schema changes or returns unexpected shapes.
    """
    global _ipu_chamber_cache
    if _ipu_chamber_cache is not None:
        return _ipu_chamber_cache

    print("  [IPU] Pre-fetching chamber data for all countries (one-time)...")
    cache: Dict[str, List[Dict[str, Any]]] = {}

    all_iso2 = [c["iso2"] for c in COUNTRIES]
    _first_success_logged = False

    for iso2 in all_iso2:
        if iso2 in IPU_ISO2_OVERRIDES and IPU_ISO2_OVERRIDES[iso2] is None:
            print(f"  [IPU] Skipping {iso2} (explicit override: not in IPU)")
            continue

        url = f"{IPU_API_BASE}/chambers/{iso2.upper()}"
        data = req_json(url, label=f"IPU /chambers/{iso2.upper()}")

        if not data:
            print(f"  [IPU] {iso2}: no data returned (None)")
            continue

        # --- DIAGNOSTIC: log raw shape for first successful response ---
        if not _first_success_logged:
            shape_desc = (
                f"type={type(data).__name__}"
            )
            if isinstance(data, dict):
                top_keys = list(data.keys())[:10]
                shape_desc += f", top_keys={top_keys}"
                raw_inner = data.get("data")
                shape_desc += f", data_field_type={type(raw_inner).__name__}"
                if isinstance(raw_inner, list) and raw_inner:
                    shape_desc += f", data[0]_keys={list(raw_inner[0].keys())[:10]}"
                elif isinstance(raw_inner, dict):
                    shape_desc += f", data_dict_keys={list(raw_inner.keys())[:10]}"
            elif isinstance(data, list) and data:
                shape_desc += f", list_len={len(data)}, [0]_type={type(data[0]).__name__}"
                if isinstance(data[0], dict):
                    shape_desc += f", [0]_keys={list(data[0].keys())[:10]}"
            print(f"  [IPU] FIRST SUCCESS SHAPE ({iso2}): {shape_desc}")
            _first_success_logged = True

        # Parse response — handles multiple possible shapes:
        #   JSON:API single: {"data": {"attributes": {...}}}
        #   JSON:API list:   {"data": [{"attributes": {...}}, ...]}
        #   Flat dict:       {"country_code": "...", ...}
        #   Flat list:       [{...}, ...]
        chambers: List[Dict] = []

        if isinstance(data, list):
            chambers = [r for r in data if isinstance(r, dict)]
        elif isinstance(data, dict):
            raw = data.get("data")
            if isinstance(raw, list):
                chambers = [r for r in raw if isinstance(r, dict)]
            elif isinstance(raw, dict):
                chambers = [raw]
            elif raw is None:
                # No "data" key — the dict itself may be a flat chamber object
                if "country_code" in data or "last_election_date" in data or "expect_date_next_election" in data:
                    chambers = [data]
                else:
                    print(f"  [IPU] {iso2}: dict response but no 'data' key and no known flat fields. Keys: {list(data.keys())[:15]}")

        # Unwrap JSON:API attributes if present
        unwrapped: List[Dict] = []
        for ch in chambers:
            raw_attrs = ch.get("attributes")
            unwrapped.append(raw_attrs if isinstance(raw_attrs, dict) else ch)

        if unwrapped:
            print(f"  [IPU] {iso2}: found {len(unwrapped)} chamber(s)")
            # Log available date fields on first chamber for debugging
            first_ch = unwrapped[0]
            date_fields = {k: v for k, v in first_ch.items() if "date" in k.lower() or "election" in k.lower()}
            print(f"  [IPU] {iso2}: date/election fields = {date_fields}")
            cache[iso2.upper()] = unwrapped
        else:
            print(f"  [IPU] {iso2}: response parsed to 0 chambers. Raw type: {type(data).__name__}")

        time.sleep(0.15)  # be polite to IPU

    print(f"  [IPU] Cached chamber data for {len(cache)} countries.")
    _ipu_chamber_cache = cache
    return cache

def _parse_ipu_date(raw: Any) -> Optional[str]:
    """
    Normalize IPU date values to ISO date string (YYYY-MM-DD) or None.
    IPU may return dates as strings like "2026-03", "2026", "2026-03-15",
    or as dicts with a "value" key.
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        raw = raw.get("value") or raw.get("date") or raw.get("text")
    if not raw:
        return None
    s = str(raw).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    if re.match(r"^\d{4}-\d{2}$", s):
        return s
    if re.match(r"^\d{4}$", s):
        return s
    m = re.match(r"^(\d{4}-\d{2}-\d{2})T", s)
    if m:
        return m.group(1)
    return s or None


def fetch_ipu_legislative_election(iso2: str) -> Dict[str, Any]:
    """
    Returns legislative election data for a country from the IPU Parline cache.
    """
    if iso2 in IPU_ISO2_OVERRIDES:
        override = IPU_ISO2_OVERRIDES[iso2]
        if override is None:
            return {
                "exists": "unknown",
                "lastDate": None,
                "nextDate": None,
                "chamberName": None,
                "chamberType": None,
                "method": "ipu_parline",
                "notes": f"Country ({iso2}) not represented in IPU Parline.",
            }

    cache = _load_ipu_cache()
    chambers = cache.get(iso2.upper(), [])

    if not chambers:
        return {
            "exists": "unknown",
            "lastDate": None,
            "nextDate": None,
            "chamberName": None,
            "chamberType": None,
            "method": "ipu_parline",
            "notes": f"No IPU chamber data found for {iso2}.",
        }

    def _chamber_priority(c: dict) -> int:
        status = str(c.get("struct_parl_status") or "").lower()
        if "lower" in status or "unicameral" in status:
            return 0
        if "upper" in status:
            return 1
        return 2

    chambers_sorted = sorted(chambers, key=_chamber_priority)
    best = chambers_sorted[0]
    is_suspended = best.get("is_suspended_chamber", False)

    next_date = _parse_ipu_date(best.get("expect_date_next_election"))
    last_date = _parse_ipu_date(best.get("last_election_date"))
    exists: Any = True if (next_date or last_date) else "unknown"

    return {
        "exists": exists,
        "lastDate": last_date,
        "nextDate": next_date,
        "chamberName": best.get("election_title") or best.get("country_name"),
        "chamberType": best.get("struct_parl_status"),
        "method": "ipu_parline",
        "notes": (
            "Chamber suspended per IPU Parline." if is_suspended
            else "From IPU Parline parliamentary election schedule."
        ),
    }


# ---------------------------- REST COUNTRIES ----------------------------

def fetch_rest_countries_metadata(iso2: str) -> Dict[str, Any]:
    """
    Fetches country metadata from the REST Countries API.
    NOTE: Do NOT pass a fields param — when fields is specified, REST Countries v3.1
    returns a plain dict instead of a list, which breaks list-unwrap.
    """
    url = f"{REST_COUNTRIES_BASE}/alpha/{iso2.lower()}"
    data = req_json(url, label=f"REST Countries /alpha/{iso2.lower()}")

    # REST Countries /alpha/{code} normally returns a list with one item
    if isinstance(data, list):
        data = data[0] if data else None
    elif not isinstance(data, dict):
        data = None

    if not data:
        return {
            "capital": None,
            "population": None,
            "region": None,
            "subregion": None,
            "flag": None,
            "flagPng": None,
            "currencies": [],
            "languages": [],
            "officialName": None,
            "source": "restcountries",
            "notes": f"No REST Countries data for {iso2}.",
        }

    # Capital: returned as a list
    capital_raw = data.get("capital")
    capital = capital_raw[0] if isinstance(capital_raw, list) and capital_raw else None

    # Currencies: dict keyed by currency code, values have "name"
    currencies_raw = data.get("currencies") or {}
    currencies = [v.get("name") for v in currencies_raw.values() if isinstance(v, dict) and v.get("name")]

    # Languages: dict keyed by language code, values are language name strings
    languages_raw = data.get("languages") or {}
    languages = list(languages_raw.values()) if isinstance(languages_raw, dict) else []

    # Official name
    name_obj = data.get("name") or {}
    official_name = name_obj.get("official") if isinstance(name_obj, dict) else None

    # Flag PNG URL
    flags_obj = data.get("flags") or {}
    flag_png = flags_obj.get("png") if isinstance(flags_obj, dict) else None

    return {
        "capital": capital,
        "population": data.get("population"),
        "region": data.get("region"),
        "subregion": data.get("subregion"),
        "flag": data.get("flag"),
        "flagPng": flag_png,
        "currencies": currencies,
        "languages": languages,
        "officialName": official_name,
        "source": "restcountries",
        "notes": None,
    }


# ---------------------------- WORLD BANK (WGI governance) ----------------------------

def _wb_indicator_url(iso2: str, indicator: str) -> str:
    iso2_l = iso2.strip().lower()
    return f"{WORLD_BANK_BASE}/country/{iso2_l}/indicator/{indicator}"

def _parse_wb_series_latest(payload: Any) -> Tuple[Optional[float], Optional[int], Optional[str]]:
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        return None, None, "Unexpected WB response shape."

    series = payload[1]
    for row in series:
        if not isinstance(row, dict):
            continue
        val = row.get("value")
        dt = row.get("date")
        if val is None or dt is None:
            continue
        try:
            year = int(dt)
        except Exception:
            year = None
        try:
            fval = float(val)
        except Exception:
            continue
        return fval, year, None

    return None, None, "No non-null value found."

def fetch_wb_indicator_latest(iso2: str, indicator: str) -> Dict[str, Any]:
    url = _wb_indicator_url(iso2, indicator)
    payload = req_json(url, params={"format": "json", "per_page": 60}, label=f"WB {indicator} {iso2}")
    if payload is None:
        return {"ok": False, "value": None, "year": None, "source": url, "notes": "Failed to fetch WB indicator."}

    value, year, notes = _parse_wb_series_latest(payload)
    if value is None or year is None:
        return {"ok": False, "value": None, "year": None, "source": url, "notes": notes or "Could not parse WB indicator."}

    return {"ok": True, "value": value, "year": year, "source": url, "notes": None}

def fetch_wb_wgi_percentiles(iso2: str) -> Dict[str, Any]:
    components: Dict[str, Any] = {}
    years: List[int] = []
    values: List[float] = []
    sources: Dict[str, str] = {}

    for dim, code in WGI_PERCENTILE_INDICATORS.items():
        res = fetch_wb_indicator_latest(iso2, code)
        sources[dim] = res.get("source")
        if res.get("ok") is True and res.get("value") is not None and res.get("year") is not None:
            v = float(res["value"])
            y = int(res["year"])
            components[dim] = {
                "indicator": code,
                "percentile": v,
                "label": percentile_to_label(v, dim),
                "year": y,
            }
            years.append(y)
            values.append(v)
        else:
            components[dim] = {
                "indicator": code,
                "percentile": None,
                "label": None,
                "year": None,
                "notes": res.get("notes"),
            }

    if not values:
        return {
            "ok": False,
            "overallPercentile": None,
            "band": "unknown",
            "bandLabel": None,
            "year": None,
            "components": components,
            "sources": sources,
            "notes": "No WGI percentile values available (WB may not have this entity / code).",
        }

    overall = sum(values) / len(values)
    yr = max(years) if years else None
    return {
        "ok": True,
        "overallPercentile": round(overall, 2),
        "band": percentile_to_tier(overall),
        "bandLabel": overall_percentile_to_label(overall),
        "year": yr,
        "components": components,
        "sources": sources,
        "notes": None,
    }

def merge_wb_sticky(new_wb: Dict[str, Any], prev_country_obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    prev_wb = (prev_country_obj or {}).get("worldBankGovernance") if isinstance(prev_country_obj, dict) else None
    prev_overall = prev_wb.get("overallPercentile") if isinstance(prev_wb, dict) else None

    if new_wb.get("ok") is True:
        out = dict(new_wb)
        out.pop("ok", None)
        return out

    if prev_overall is not None:
        kept = dict(prev_wb)
        kept["notes"] = f"Kept previous WB governance values because latest fetch failed: {new_wb.get('notes')}"
        return kept

    out = dict(new_wb)
    out.pop("ok", None)
    return out


# ---------------------------- BUILD ----------------------------

def build_country(country_name: str, iso2: str, prev_by_iso2: Dict[str, Any]) -> Dict[str, Any]:
    print(f"  [{iso2}] Fetching Wikidata QID...")
    qid = get_wikidata_country_qid_by_iso2(iso2)
    print(f"  [{iso2}] QID = {qid}")

    political_systems: List[str] = []
    gov: Dict[str, Any] = {
        "headOfState": {"name": None, "party": None},
        "headOfGovernment": {"name": None, "party": None},
        "legislatureBodies": [],
        "executiveController": {"leader": None, "partyOrGroup": "unknown", "method": "hog_party_else_hos_party"},
    }
    elections_exec = {
        "exists": "unknown",
        "nextDate": None,
        "electionType": None,
        "method": "wikidata_upcoming",
        "notes": "QID not found",
    }
    leg_control = {
        "winner": "unknown",
        "method": "wikidata_last_leg_election_winner",
        "notes": "QID not found",
    }

    if qid:
        print(f"  [{iso2}] Fetching political systems...")
        political_systems = get_political_system_labels(qid) or ["unknown"]
        print(f"  [{iso2}] Fetching government snapshot...")
        gov = get_government_snapshot(qid)
        print(f"  [{iso2}] Fetching executive elections...")
        elections_exec = get_next_election_upcoming_wikidata(qid, "executive")
        print(f"  [{iso2}] Fetching last leg election winner...")
        leg_control = get_last_legislative_election_winner(qid)
    else:
        political_systems = ["unknown"]

    # --- IPU Parline: legislative election data (primary source) ---
    print(f"  [{iso2}] Fetching IPU legislative elections...")
    ipu_leg = fetch_ipu_legislative_election(iso2)
    print(f"  [{iso2}] IPU result: lastDate={ipu_leg.get('lastDate')}, nextDate={ipu_leg.get('nextDate')}")

    if ipu_leg.get("nextDate"):
        elections_leg = {
            "exists": ipu_leg["exists"],
            "lastDate": ipu_leg["lastDate"],
            "nextDate": ipu_leg["nextDate"],
            "electionType": ipu_leg["chamberType"],
            "method": "ipu_parline",
            "notes": ipu_leg["notes"],
            "source": "IPU Parline API",
        }
    else:
        wd_leg_fallback: Dict[str, Any] = {}
        if qid:
            print(f"  [{iso2}] IPU had no nextDate — trying Wikidata legislative fallback...")
            wd_leg_fallback = get_next_election_upcoming_wikidata(qid, "legislative")
        elections_leg = {
            "exists": wd_leg_fallback.get("exists", "unknown"),
            "lastDate": ipu_leg.get("lastDate"),
            "nextDate": wd_leg_fallback.get("nextDate"),
            "electionType": wd_leg_fallback.get("electionType"),
            "method": "ipu_parline+wikidata_fallback",
            "notes": (
                f"IPU Parline had no next date ({ipu_leg.get('notes', '')}); "
                f"next date from Wikidata fallback."
            ),
            "source": "IPU Parline (last date) + Wikidata (next date fallback)",
        }

    bodies = gov.get("legislatureBodies") or []
    if not bodies:
        bodies = ["Legislature"]

    legislature = []
    for b in bodies:
        legislature.append({
            "name": b,
            "inControl": leg_control.get("winner", "unknown"),
            "controlMethod": leg_control.get("method"),
            "controlNotes": leg_control.get("notes"),
            "controlBasis": leg_control.get("basis"),
        })

    print(f"  [{iso2}] Fetching World Bank WGI...")
    new_wb = fetch_wb_wgi_percentiles(iso2)
    prev_obj = prev_by_iso2.get(iso2)
    wb_gov = merge_wb_sticky(new_wb, prev_obj)

    print(f"  [{iso2}] Fetching REST Countries metadata...")
    metadata = fetch_rest_countries_metadata(iso2)
    print(f"  [{iso2}] metadata.capital={metadata.get('capital')}, metadata.population={metadata.get('population')}")

    # --- Build dataAvailability block ---
    static_notes = DATA_AVAILABILITY_NOTES.get(iso2, {})
    availability: Dict[str, Any] = {}

    if not qid:
        availability["executive"] = (
            static_notes.get("executive") or
            f"No Wikidata QID found for ISO2 '{iso2}'. "
            "Executive data (head of state/government, political system) could not be fetched."
        )
    elif static_notes.get("executive"):
        availability["executive"] = static_notes["executive"]

    if wb_gov.get("overallPercentile") is None:
        availability["worldBankGovernance"] = (
            static_notes.get("worldBankGovernance") or
            f"World Bank WGI data unavailable for '{iso2}'."
        )
    elif static_notes.get("worldBankGovernance"):
        availability["worldBankGovernance"] = static_notes["worldBankGovernance"]

    if elections_leg.get("nextDate") is None and elections_leg.get("lastDate") is None:
        availability["elections.legislative"] = (
            static_notes.get("elections.legislative") or
            f"No legislative election data found in IPU Parline or Wikidata for '{iso2}'."
        )
    elif static_notes.get("elections.legislative"):
        availability["elections.legislative"] = static_notes["elections.legislative"]

    if metadata.get("capital") is None and metadata.get("population") is None:
        availability["metadata"] = (
            static_notes.get("metadata") or
            f"REST Countries API returned no data for '{iso2}'."
        )

    # --- Apply static executive overrides ---
    ov = STATIC_EXECUTIVE_OVERRIDES.get(iso2, {})
    if ov:
        print(f"  [{iso2}] Applying static executive override: {list(ov.keys())}")
    hos_name  = ov.get("hosName")  or gov["headOfState"].get("name")
    hos_party = ov.get("hosParty") or gov["headOfState"].get("party") or "unknown"
    hog_name  = ov.get("hogName")  or gov["headOfGovernment"].get("name")
    hog_party = ov.get("hogParty") or gov["headOfGovernment"].get("party") or "unknown"
    exec_leader = hog_name or hos_name
    exec_party  = hog_party if hog_party != "unknown" else hos_party
    exec_source = "static_override" if ov else "wikidata"

    return {
        "country": country_name,
        "iso2": iso2,
        "metadata": {
            "officialName": metadata["officialName"],
            "capital": metadata["capital"],
            "population": metadata["population"],
            "region": metadata["region"],
            "subregion": metadata["subregion"],
            "flag": metadata["flag"],
            "flagPng": metadata["flagPng"],
            "currencies": metadata["currencies"],
            "languages": metadata["languages"],
            "source": metadata["source"],
        },
        "politicalSystem": {
            "values": political_systems or ["unknown"],
            "source": "wikidata:P122",
        },
        "executive": {
            "headOfState": {
                "name": hos_name,
                "partyOrGroup": hos_party,
                "source": (
                    f"wikidata:P35 + static_override:True"
                    if ov else
                    "wikidata:P35 (current statement; +party P102)"
                ),
            },
            "headOfGovernment": {
                "name": hog_name,
                "partyOrGroup": hog_party,
                "source": (
                    f"wikidata:P6 + static_override:True"
                    if ov else
                    "wikidata:P6 (current statement; +party P102)"
                ),
            },
            "executiveInPower": {
                "leader": exec_leader,
                "partyOrGroup": exec_party,
                "method": exec_source,
            },
        },
        "legislature": {
            "bodies": legislature,
            "source": "wikidata:P194 (filtered to legislature items) + control best-effort via elections winner P1346",
        },
        "worldBankGovernance": wb_gov,
        "dataAvailability": availability if availability else None,
        "elections": {
            "legislative": elections_leg,
            "executive": {
                "exists": elections_exec["exists"],
                "nextDate": elections_exec["nextDate"],
                "electionType": elections_exec["electionType"],
                "method": elections_exec["method"],
                "notes": elections_exec["notes"],
                "source": "wikidata:P1001+P17 UNION, P585, P31 (tightened types)",
            },
        },
    }


def main() -> None:
    out_path = Path("public") / "countries_snapshot.json"
    prev_by_iso2 = load_previous_snapshot(out_path)

    print("=== Starting countries snapshot build ===")
    print(f"  Previous snapshot: {len(prev_by_iso2)} countries cached")

    # Pre-warm the IPU cache once before processing all countries
    _load_ipu_cache()

    out = {
        "generatedAt": iso_z(now_utc()),
        "worldBankYearRule": "latest_non_null_per_indicator",
        "countries": [],
        "sources": {
            "wikidata_sparql": WIKIDATA_SPARQL,
            "world_bank_base": WORLD_BANK_BASE,
            "ipu_parline": IPU_API_BASE,
            "rest_countries": REST_COUNTRIES_BASE,
        },
        "worldBankIndicatorsUsed": WGI_PERCENTILE_INDICATORS,
    }

    for c in COUNTRIES:
        name = c["country"]
        iso2 = c["iso2"]
        print(f"\n▶ Building {name} ({iso2}) ...")
        out["countries"].append(build_country(name, iso2, prev_by_iso2))
        time.sleep(0.25)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ Wrote {len(out['countries'])} countries to {out_path.resolve()}")


if __name__ == "__main__":
    main()
