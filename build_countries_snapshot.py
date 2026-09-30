"""
Build a Base44-friendly JSON snapshot for multiple countries.  (No AI / no API keys.)
Output: docs/countries_snapshot.json

Run:  python build_countries_snapshot.py
Deps: pip install requests
Env:  RESTCOUNTRIES_API_KEY (free key from restcountries.com)

Sources (all free):
  - Metadata:   REST Countries v5 API, free key required (capital, population, flag, currencies, languages, official name)
  - Governance: World Bank WGI API (refreshed when missing or >30 days old)
  - Executives: Wikipedia "List of current heads of state and government"
  - Everything else (legislature, elections, party profiles, political system)
    is carried forward from the previous snapshot untouched, since it was
    originally written by an earlier AI-assisted version and nothing updates it anymore.
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 25
MAX_RETRIES = 3
RETRY_SLEEP = 1.5
WGI_REFRESH_DAYS = 30

WORLD_BANK_BASE = "https://api.worldbank.org/v2"
REST_COUNTRIES_BASE = "https://api.restcountries.com/countries/v5"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"

WGI_PERCENTILE_INDICATORS: Dict[str, str] = {
    "voiceAccountability":     "GOV_WGI_VA.SC",
    "politicalStability":      "GOV_WGI_PV.SC",
    "governmentEffectiveness": "GOV_WGI_GE.SC",
    "regulatoryQuality":       "GOV_WGI_RQ.SC",
    "ruleOfLaw":               "GOV_WGI_RL.SC",
    "controlOfCorruption":     "GOV_WGI_CC.SC",
}
_DIM_NAMES = {
    "voiceAccountability": "voice & accountability",
    "politicalStability": "political stability",
    "governmentEffectiveness": "government effectiveness",
    "regulatoryQuality": "regulatory quality",
    "ruleOfLaw": "rule of law",
    "controlOfCorruption": "control of corruption",
}
_TIER_WORD = {"Very Low": "Very low", "Low": "Low", "Medium": "Moderate",
              "High": "High", "Very High": "Very high"}
WB_ISO2_OVERRIDES = {"XK": "XKX", "TW": "TWN"}

_C = [
    "Ukraine:UA", "Russia:RU", "India:IN", "Pakistan:PK", "China:CN", "United Kingdom:GB",
    "Germany:DE", "UAE:AE", "Saudi Arabia:SA", "Israel:IL", "Palestine:PS", "Mexico:MX",
    "Brazil:BR", "Canada:CA", "Nigeria:NG", "Japan:JP", "Iran:IR", "Syria:SY", "France:FR",
    "Turkey:TR", "Venezuela:VE", "Vietnam:VN", "Taiwan:TW", "South Korea:KR", "North Korea:KP",
    "Indonesia:ID", "Myanmar:MM", "Armenia:AM", "Azerbaijan:AZ", "Morocco:MA", "Somalia:SO",
    "Yemen:YE", "Libya:LY", "Egypt:EG", "Algeria:DZ", "Argentina:AR", "Chile:CL", "Peru:PE",
    "Cuba:CU", "Colombia:CO", "Panama:PA", "El Salvador:SV", "Denmark:DK", "Sudan:SD",
    "Spain:ES", "Italy:IT", "Poland:PL", "Portugal:PT", "Czech Republic:CZ", "Norway:NO",
    "Romania:RO", "Sweden:SE", "Finland:FI", "Switzerland:CH", "Netherlands:NL", "Belgium:BE",
    "Ireland:IE", "Austria:AT", "Belarus:BY", "Hungary:HU", "Serbia:RS", "Albania:AL",
    "Bulgaria:BG", "Moldova:MD", "Greece:GR", "Croatia:HR", "Slovakia:SK", "Slovenia:SI",
    "Lithuania:LT", "Latvia:LV", "Estonia:EE", "North Macedonia:MK", "Bosnia and Herzegovina:BA",
    "Montenegro:ME", "Luxembourg:LU", "Iceland:IS", "Malta:MT", "Cyprus:CY", "Georgia:GE",
    "Kosovo:XK", "Hong Kong:HK", "Iraq:IQ", "Jordan:JO", "Lebanon:LB", "Kuwait:KW",
    "Bahrain:BH", "Oman:OM", "Qatar:QA", "Afghanistan:AF", "Turkmenistan:TM", "Kazakhstan:KZ",
    "Uzbekistan:UZ", "Kyrgyzstan:KG", "Tajikistan:TJ", "Australia:AU", "New Zealand:NZ",
    "Singapore:SG", "Philippines:PH", "Malaysia:MY", "Thailand:TH", "Cambodia:KH", "Laos:LA",
    "Bangladesh:BD", "Nepal:NP", "Sri Lanka:LK", "Mongolia:MN", "Brunei:BN", "Timor-Leste:TL",
    "Maldives:MV", "Bhutan:BT", "Papua New Guinea:PG", "Angola:AO", "South Africa:ZA",
    "Kenya:KE", "DRC:CD", "Congo:CG", "Tunisia:TN", "Ethiopia:ET", "Ghana:GH", "Ivory Coast:CI",
    "Senegal:SN", "Rwanda:RW", "Uganda:UG", "Zimbabwe:ZW", "Zambia:ZM", "Cameroon:CM",
    "Mozambique:MZ", "Burkina Faso:BF", "Niger:NE", "Chad:TD", "Guinea:GN", "Mali:ML",
    "Botswana:BW", "Tanzania:TZ", "Madagascar:MG", "South Sudan:SS", "Eritrea:ER",
    "Djibouti:DJ", "Mauritania:MR", "Liberia:LR", "Sierra Leone:SL", "Gabon:GA", "Namibia:NA",
    "Eswatini:SZ", "Lesotho:LS", "Malawi:MW", "Bolivia:BO", "Ecuador:EC", "Paraguay:PY",
    "Uruguay:UY", "Guyana:GY", "Dominican Republic:DO", "Guatemala:GT", "Honduras:HN",
    "Nicaragua:NI", "Costa Rica:CR", "Haiti:HT", "Trinidad and Tobago:TT", "Jamaica:JM",
    "Bahamas:BS",
]
COUNTRIES: List[Dict[str, str]] = [
    {"country": s.rsplit(":", 1)[0], "iso2": s.rsplit(":", 1)[1]} for s in _C
]

# Wikipedia's spelling where it differs from our display name
WIKI_ALIASES = {
    "AE": "United Arab Emirates", "CD": "Democratic Republic of the Congo",
    "CG": "Republic of the Congo", "CZ": "Czechia", "TR": "Türkiye",
    "MK": "North Macedonia", "CI": "Ivory Coast", "TL": "East Timor",
}

SOVEREIGNTY_NOTES = {
    "TW": "Taiwan is a self-governing democracy not recognized as a sovereign state by most UN members.",
    "HK": "Hong Kong is a Special Administrative Region of China ('one country, two systems').",
    "XK": "Kosovo declared independence in 2008; recognised by roughly 100 UN members, not by Serbia, Russia or China.",
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def req_json(url: str, params: Optional[dict] = None, label: str = "") -> Optional[Any]:
    tag = label or url
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                print(f"    [req_json] {tag} → HTTP {r.status_code}")
                return None
            print(f"    [req_json] {tag} → HTTP {r.status_code} (attempt {attempt}/{MAX_RETRIES})")
        except (requests.RequestException, ValueError) as exc:
            print(f"    [req_json] {tag} → error attempt {attempt}/{MAX_RETRIES}: {exc}")
        time.sleep(RETRY_SLEEP * attempt)
    print(f"    [req_json] {tag} → all retries exhausted")
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
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

# ── WIKIPEDIA HEADS OF STATE / GOVERNMENT ─────────────────────────────────────

_TITLE_RE = re.compile(
    r"^(?:President|Prime\s+Minister|King|Queen|Emperor|Chancellor|"
    r"General\s+Secretary(?:\s+of\s+the\s+Communist\s+Party)?|"
    r"First\s+Secretary(?:\s+of\s+the\s+Communist\s+Party)?|"
    r"Premier|Governor[\s-]General|Grand\s+Duke)"
    r"(?:\s*\[\s*\w+\s*\])*\s*[–—-]\s*", re.IGNORECASE)

def _clean_wiki(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = _TITLE_RE.sub("", s)
    s = re.sub(r"\s*\[\s*[^\]]*\]\s*", " ", s).strip()
    return s or None

class _TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_cell = False
        self.row: List[str] = []
        self.parts: List[str] = []
        self.rows: List[List[str]] = []

    def _text(self) -> str:
        raw = re.sub(r"\[\d+\]", "", " ".join(self.parts))
        return re.sub(r"\s+", " ", raw).strip()

    def handle_starttag(self, tag, attrs):
        if tag == "table" and "wikitable" in dict(attrs).get("class", ""):
            self.in_table = True
        if not self.in_table:
            return
        if tag == "tr":
            self.row = []
        if tag in ("td", "th"):
            self.in_cell, self.parts = True, []
        if tag == "br":
            self.parts.append(" | ")

    def handle_endtag(self, tag):
        if not self.in_table:
            return
        if tag in ("td", "th") and self.in_cell:
            self.row.append(self._text())
            self.in_cell, self.parts = False, []
        if tag == "tr" and self.row:
            self.rows.append(self.row)
            self.row = []
        if tag == "table":
            self.in_table = False

    def handle_data(self, data):
        if self.in_cell:
            self.parts.append(data)

_wiki_cache: Optional[Dict[str, Dict[str, Optional[str]]]] = None

def _norm(s: str) -> str:
    return re.sub(r"\s*\(.*?\)\s*", " ", s).strip().lower()

def load_wiki_exec_cache() -> Dict[str, Dict[str, Optional[str]]]:
    global _wiki_cache
    if _wiki_cache is not None:
        return _wiki_cache
    _wiki_cache = {}
    print("  [WIKI] Fetching heads of state/government list...")
    data = req_json(WIKIPEDIA_API, params={
        "action": "parse", "page": "List of current heads of state and government",
        "prop": "text", "format": "json", "formatversion": "2", "disableeditsection": "1",
    }, label="Wikipedia HOS/HOG list")
    html_text = safe_get(data or {}, "parse", "text", default="")
    if not html_text:
        print("  [WIKI] Failed — executives will be carried forward")
        return _wiki_cache

    rev: Dict[str, str] = {}
    for c in COUNTRIES:
        rev[_norm(c["country"])] = c["iso2"]
    for iso2, name in WIKI_ALIASES.items():
        rev[_norm(name)] = iso2

    def first(s: str) -> Optional[str]:
        parts = [p.strip() for p in s.split("|") if p.strip()]
        return parts[0] if parts else None

    parser = _TableParser()
    parser.feed(html_text)
    for row in parser.rows:
        if len(row) < 2:
            continue
        iso2 = rev.get(_norm(row[0]))
        if not iso2:
            continue
        hos = first(row[1])
        hog = first(row[2]) if len(row) > 2 else hos
        _wiki_cache[iso2] = {"hosName": hos, "hogName": hog}
    print(f"  [WIKI] Parsed {len(_wiki_cache)} countries")
    return _wiki_cache

# ── REST COUNTRIES ────────────────────────────────────────────────────────────

def fetch_rest_countries(iso2: str) -> Dict[str, Any]:
    """REST Countries v5 (v3.1 was shut down). Needs a free key in RESTCOUNTRIES_API_KEY."""
    key = os.environ.get("RESTCOUNTRIES_API_KEY", "").strip()
    if not key:
        print("    [REST] RESTCOUNTRIES_API_KEY not set — keeping previous metadata")
        return {}
    url = f"{REST_COUNTRIES_BASE}/codes.alpha_2/{iso2.upper()}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
                             timeout=TIMEOUT)
            if r.status_code == 429:           # free plan: 20 req / 10 s
                time.sleep(5 * attempt)
                continue
            if r.status_code != 200:
                print(f"    [REST] {iso2} → HTTP {r.status_code}")
                return {}
            objs = safe_get(r.json(), "data", "objects", default=[])
            break
        except (requests.RequestException, ValueError) as exc:
            print(f"    [REST] {iso2} → error attempt {attempt}/{MAX_RETRIES}: {exc}")
            time.sleep(RETRY_SLEEP * attempt)
    else:
        return {}
    if not objs:
        return {}
    d = objs[0]
    names = d.get("names") or {}
    caps = d.get("capitals") or []
    primary = next((c for c in caps if (c.get("attributes") or {}).get("primary")), caps[0] if caps else None)
    curr = d.get("currencies") or {}
    langs = d.get("languages") or {}
    flag = d.get("flag") or {}
    return {
        "officialName": names.get("official"),
        "capital":      primary.get("name") if primary else None,
        "population":   d.get("population"),
        "region":       d.get("region"),
        "subregion":    d.get("subregion"),
        "flag":         flag.get("emoji"),
        "flagPng":      flag.get("url_png"),
        "currencies":   [v["name"] for v in curr.values() if isinstance(v, dict) and v.get("name")],
        "languages":    list(langs.values()) if isinstance(langs, dict) else [],
        "source":       "restcountries_v5",
    }

# ── WORLD BANK WGI ────────────────────────────────────────────────────────────

def tier(p: Optional[float]) -> Optional[str]:
    if p is None:
        return None
    for cutoff, t in ((20, "Very Low"), (40, "Low"), (60, "Medium"), (80, "High")):
        if p < cutoff:
            return t
    return "Very High"

def _parse_wb(payload: Any) -> Tuple[Optional[float], Optional[int]]:
    rows = payload[1] if isinstance(payload, list) and len(payload) >= 2 and isinstance(payload[1], list) else []
    for row in rows:
        if isinstance(row, dict) and row.get("value") is not None and row.get("date") is not None:
            try:
                return float(row["value"]), int(str(row["date"])[:4])
            except (ValueError, TypeError):
                continue
    return None, None

def fetch_wgi(iso2: str) -> Dict[str, Any]:
    wb = WB_ISO2_OVERRIDES.get(iso2.upper(), iso2.upper())
    components, values, years, sources = {}, [], [], {}
    for dim, code in WGI_PERCENTILE_INDICATORS.items():
        url = f"{WORLD_BANK_BASE}/country/{wb}/indicator/{code}"
        sources[dim] = url
        payload = req_json(url, params={"source": "3", "format": "json", "mrv": 1}, label=f"WB {code} {iso2}")
        v, y = _parse_wb(payload) if payload else (None, None)
        if v is None:
            components[dim] = {"indicator": code, "percentile": None, "label": None, "year": None,
                               "notes": "No value from World Bank."}
            continue
        t = tier(v)
        components[dim] = {"indicator": code, "percentile": v,
                           "label": f"{_TIER_WORD[t]} {_DIM_NAMES[dim]}", "year": y}
        values.append(v)
        years.append(y)
    if not values:
        return {"ok": False, "notes": "No WGI values available."}
    overall = sum(values) / len(values)
    t = tier(overall)
    return {"ok": True, "overallPercentile": round(overall, 2), "band": t,
            "bandLabel": f"{_TIER_WORD[t]} governance overall", "year": max(years),
            "components": components, "sources": sources, "notes": None,
            "fetchedAt": iso_z(now_utc())}

def _wgi_is_fresh(prev_wb: Optional[Dict]) -> bool:
    if not prev_wb or prev_wb.get("overallPercentile") is None:
        return False
    ts = prev_wb.get("fetchedAt")
    if not ts:
        return False
    try:
        last = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return (now_utc() - last).days < WGI_REFRESH_DAYS
    except ValueError:
        return False

def get_wgi(iso2: str, prev: Optional[Dict]) -> Dict[str, Any]:
    prev_wb = (prev or {}).get("worldBankGovernance")
    if _wgi_is_fresh(prev_wb):
        return prev_wb
    new = fetch_wgi(iso2)
    if new.pop("ok"):
        return new
    if prev_wb and prev_wb.get("overallPercentile") is not None:
        kept = dict(prev_wb)
        kept["notes"] = f"Kept previous values; latest fetch failed: {new.get('notes')}"
        return kept
    return {"overallPercentile": None, "band": "unknown", "bandLabel": None, "year": None,
            "components": {}, "sources": {}, "notes": new.get("notes")}

# ── BUILD ONE COUNTRY ─────────────────────────────────────────────────────────

def _executive(prev: Optional[Dict], wiki: Dict) -> Dict:
    prev_exec = (prev or {}).get("executive") or {}
    out = {}
    for key, wkey in (("headOfState", "hosName"), ("headOfGovernment", "hogName")):
        old = prev_exec.get(key) or {}
        new_name = _clean_wiki(wiki.get(wkey))
        if new_name and new_name != old.get("name"):
            out[key] = {"name": new_name, "partyOrGroup": None, "source": "wikipedia"}
        elif old:
            out[key] = old
        else:
            out[key] = {"name": new_name, "partyOrGroup": None, "source": "wikipedia" if new_name else None}
    hos, hog = out["headOfState"], out["headOfGovernment"]
    leader = hog.get("name") or hos.get("name")
    src = hog if hog.get("name") else hos
    out["executiveInPower"] = {
        "leader": leader, "partyOrGroup": src.get("partyOrGroup"),
        "method": "head_of_government" if hog.get("name") and hog.get("name") != hos.get("name") else "head_of_state",
    }
    return out

def _empty_elections() -> Dict:
    return {
        "competitiveElections": None, "nonCompetitiveReason": None,
        "electionsSuspended": False, "suspensionReason": None,
        "lastCompetitivenessCheck": None, "ipu_not_applicable": False,
        "ipu_not_applicable_reason": None, "electionToday": False,
        "electionWatchActive": False, "electionWatchReason": None,
        "legislative": {"lastElection": None, "nextElection": None, "source": "unknown"},
        "executive":   {"lastElection": None, "nextElection": None, "source": "unknown"},
    }

def build_country(name: str, iso2: str, prev: Optional[Dict], wiki_all: Dict) -> Dict[str, Any]:
    prev_meta = (prev or {}).get("metadata") or {}
    meta = fetch_rest_countries(iso2)
    if not meta:  # keep last good metadata if the API hiccups
        meta = dict(prev_meta) if prev_meta else {}
    wb_gov = get_wgi(iso2, prev)

    elections = dict((prev or {}).get("elections") or _empty_elections())
    # Nothing manages the election watch any more — don't leave it stuck on.
    elections["electionWatchActive"] = False
    elections["electionWatchReason"] = None

    avail: Dict[str, str] = {}
    note = SOVEREIGNTY_NOTES.get(iso2)
    if note:
        avail["executive"] = note
    if wb_gov.get("overallPercentile") is None:
        avail["worldBankGovernance"] = f"World Bank WGI data unavailable for '{iso2}'."
    if not meta.get("capital") and not meta.get("population"):
        avail["metadata"] = f"REST Countries API returned no data for '{iso2}'."

    return {
        "country": name,
        "iso2": iso2,
        "metadata": {k: meta.get(k) for k in (
            "officialName", "capital", "population", "region", "subregion",
            "flag", "flagPng", "currencies", "languages", "source")},
        "politicalSystem": (prev or {}).get("politicalSystem") or {"values": ["unknown"], "source": "unknown"},
        "executive": _executive(prev, wiki_all.get(iso2, {})),
        "legislature": (prev or {}).get("legislature") or {"bodies": [], "source": "unknown"},
        "partyProfiles": (prev or {}).get("partyProfiles"),
        "worldBankGovernance": wb_gov,
        "dataAvailability": avail or None,
        "elections": elections,
    }

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    out_path = Path("docs") / "countries_snapshot.json"
    prev_full = load_previous_snapshot(out_path)
    prev_by_iso2 = {c["iso2"]: c for c in prev_full.get("countries", []) if c.get("iso2")}
    print(f"=== Starting build. Previous snapshot: {len(prev_by_iso2)} countries cached ===")

    wiki_all = load_wiki_exec_cache()

    out = {
        "generatedAt": iso_z(now_utc()),
        "worldBankYearRule": "latest_non_null_per_indicator",
        "countries": [],
        "sources": {
            "metadata": REST_COUNTRIES_BASE,
            "governance": WORLD_BANK_BASE,
            "executives": f"{WIKIPEDIA_API} (List of current heads of state and government)",
            "legislature_elections_party_profiles": "carried forward from earlier snapshots; no longer auto-updated",
        },
        "worldBankIndicatorsUsed": WGI_PERCENTILE_INDICATORS,
    }

    for c in COUNTRIES:
        print(f"▶ {c['country']} ({c['iso2']})")
        out["countries"].append(build_country(c["country"], c["iso2"], prev_by_iso2.get(c["iso2"]), wiki_all))
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote {len(out['countries'])} countries → {out_path.resolve()}")

if __name__ == "__main__":
    main()
