# """
# backend/reconciler/resolver.py — Client resolution chain.
# ================================================================================
# Responsibility: answer "Who is this payment from?"

# Contains the full resolution pipeline:
#   1. Validator — Knowledge Bank keyword/fuzzy matching
#   2. KB Bridge — maps KB official_name → PNM standardised name
#   3. Narration Parser — extracts client name from NEFT/CMS/SGB/INF/RTGS strings
#   4. ClientResolver — 4-pass chain: KB → PNM → Fuzzy → Fail

# Does NOT handle: bill matching, FIFO application, reconciliation mechanics.
# Those belong in engine.py.
# ================================================================================
# """

# import re
# import json
# import pandas as pd
# import numpy as np
# from pathlib import Path
# from typing import Optional
# from difflib import SequenceMatcher

# from backend.config import (
#     FUZZY_MATCH_THRESHOLD,
#     BRANCH_FUZZY_THRESHOLD,
# )
# from backend.common import (
#     _normalise, _normalise_for_fuzzy, _normalise_base, _fuzzy,
#     ResolutionResult, GapReason, UnresolvableReason,
# )
# from backend.parsers.debtors import standardise_party_name, extract_ref_prefix


# # ══════════════════════════════════════════════════════════════════════════════
# #  KNOWLEDGE BANK — VALIDATOR
# # ══════════════════════════════════════════════════════════════════════════════

# class Validator:
#     """
#     Matches client names/narrations against the knowledge bank.

#     Usage:
#         v = Validator("path/to/master_client_knowledge_base.json")
#         result = v.match_text("SUNDARAM HOME FINANCE")
#     """

#     FUZZY_THRESHOLD = FUZZY_MATCH_THRESHOLD

#     def __init__(self, knowledge_bank_path: str):
#         self.clients      = []
#         self.keyword_map  = {}
#         self._load(knowledge_bank_path)

#     def _load(self, path: str):
#         """Load knowledge bank from JSON and build keyword lookup map."""
#         fp = Path(path)
#         if not fp.exists():
#             print(f"⚠  Knowledge bank not found: {path}")
#             return

#         with open(fp, "r") as f:
#             self.clients = json.load(f)

#         for client in self.clients:
#             for kw in client.get("keywords", []):
#                 norm = _normalise(kw)
#                 self.keyword_map[norm] = {
#                     "id":            client["id"],
#                     "official_name": client["official_name"],
#                 }

#         print(f"✓  Knowledge bank loaded: {len(self.clients)} clients, {len(self.keyword_map)} keywords")

#     def match_text(self, text: str) -> dict:
#         """
#         Try to match a single text string against the knowledge bank.
#         3-pass: exact → substring → fuzzy.
#         """
#         unmatched = {
#             "Client_ID":     None,
#             "Official_Name": None,
#             "Match_Method":  "UNMATCHED",
#             "Match_Score":   0.0,
#         }

#         if not text or pd.isna(text):
#             return unmatched

#         norm_text = _normalise(text)

#         # Pass 1: Exact match
#         if norm_text in self.keyword_map:
#             hit = self.keyword_map[norm_text]
#             return {
#                 "Client_ID":     hit["id"],
#                 "Official_Name": hit["official_name"],
#                 "Match_Method":  "EXACT",
#                 "Match_Score":   1.0,
#             }

#         # Pass 2: Substring match
#         best_sub = None
#         best_sub_score = 0

#         for kw, hit in self.keyword_map.items():
#             if kw in norm_text or norm_text in kw:
#                 pos = norm_text.find(kw)
#                 length_score = len(kw)
#                 if pos >= 0:
#                     position_factor = 1.0 + 0.2 * max(0, 1.0 - pos / max(len(norm_text), 1))
#                 else:
#                     position_factor = 1.0
#                 score = length_score * position_factor
#                 if score > best_sub_score:
#                     best_sub_score = score
#                     best_sub = hit

#         if best_sub:
#             return {
#                 "Client_ID":     best_sub["id"],
#                 "Official_Name": best_sub["official_name"],
#                 "Match_Method":  "SUBSTRING",
#                 "Match_Score":   0.95,
#             }

#         # Pass 3: Fuzzy match
#         best_score = 0.0
#         best_hit   = None

#         text_words = set(norm_text.split())
#         for kw, hit in self.keyword_map.items():
#             kw_words = set(kw.split())
#             if not text_words & kw_words:
#                 continue

#             score = _fuzzy(norm_text, kw)
#             if score > best_score:
#                 best_score = score
#                 best_hit   = hit

#         if best_score >= self.FUZZY_THRESHOLD and best_hit:
#             return {
#                 "Client_ID":     best_hit["id"],
#                 "Official_Name": best_hit["official_name"],
#                 "Match_Method":  "FUZZY",
#                 "Match_Score":   round(best_score, 4),
#             }

#         return unmatched

#     def validate(self, df: pd.DataFrame, text_col: str) -> pd.DataFrame:
#         """Run knowledge bank matching on every row of a DataFrame."""
#         if df.empty:
#             return df

#         if text_col not in df.columns:
#             print(f"  ⚠  Column '{text_col}' not found — skipping validation")
#             return df

#         print(f"\n{'='*60}")
#         print(f"VALIDATING ({len(df)} rows, source: '{text_col}')")
#         print(f"{'='*60}")

#         results = df[text_col].apply(self.match_text)
#         result_df = pd.DataFrame(results.tolist())

#         result_df["KB_Matched"] = result_df["Client_ID"].notna()

#         drop_cols = ["Client_ID", "Official_Name", "Match_Method",
#                      "Match_Score", "KB_Matched"]
#         base = df.drop(columns=[c for c in drop_cols if c in df.columns])
#         out  = pd.concat([base.reset_index(drop=True),
#                           result_df.reset_index(drop=True)], axis=1)

#         matched   = result_df["KB_Matched"].sum()
#         unmatched = len(result_df) - matched
#         methods   = result_df[result_df["KB_Matched"]]["Match_Method"] \
#                         .value_counts().to_dict()

#         print(f"\n  Matched   : {matched:,}  ({matched/len(df)*100:.1f}%)")
#         print(f"  Unmatched : {unmatched:,}  ({unmatched/len(df)*100:.1f}%)")
#         if methods:
#             for method, count in methods.items():
#                 print(f"    {method:<12s}: {count:,}")

#         return out


# # ══════════════════════════════════════════════════════════════════════════════
# #  KB BRIDGE BUILDER
# # ══════════════════════════════════════════════════════════════════════════════

# def _build_kb_bridge(kb_path: str) -> dict:
#     """Build bridge: KB official_name → PNM standardised name."""
#     bridge = {}
#     try:
#         with open(kb_path, "r", encoding="utf-8") as f:
#             kb_clients = json.load(f)
#     except Exception:
#         return bridge

#     for client in kb_clients:
#         official = client.get("official_name", "")
#         keywords = client.get("keywords", [])

#         std = standardise_party_name(official)
#         if std and std.upper() != official.upper():
#             bridge[official.upper()] = std

#         for kw in keywords:
#             std_kw = standardise_party_name(kw)
#             if std_kw and std_kw.upper() != kw.upper():
#                 if official.upper() not in bridge:
#                     bridge[official.upper()] = std_kw
#                 break

#     return bridge


# # ══════════════════════════════════════════════════════════════════════════════
# #  NARRATION PARSER
# # ══════════════════════════════════════════════════════════════════════════════

# def _clean_candidate(candidate: str) -> str:
#     """
#     Post-extraction cleanup applied to every candidate before it is returned
#     from _parse_neft_client. Handles trailing operational suffixes and
#     alpha-numeric bleed.
#     """
#     if not candidate:
#         return candidate

#     s = candidate.strip()

#     # Step 1: Strip trailing operational suffixes (longest first)
#     _OP_SUFFIXES = [
#         'EXPENSES ACCOUNT', 'EXPENSE ACCOUNT',
#         'TREASURY ACCOUNT', 'TREASURY AC',
#         'CURRENT ACCOUNT',  'SAVINGS ACCOUNT',
#         'DISBURSEMENT AC',  'DISBURSEMENT',
#         'PAYMENT ACCOUNT',  'COLL JAIPUR',
#         'EXPENSE AC',
#     ]
#     su = s.upper()
#     for suffix in _OP_SUFFIXES:
#         if su.endswith(suffix):
#             s = s[:len(s) - len(suffix)].strip().rstrip('-').strip()
#             break

#     # Step 2: Strip trailing alpha-numeric bleed
#     s = re.sub(r'\s+[A-Z]{0,5}\d+[A-Z0-9]*\s*$', '', s, flags=re.IGNORECASE).strip()

#     # Pure-alpha truncation stub
#     _VALID_SUFFIXES = {
#         'PVT', 'LTD', 'LLP', 'INC', 'CO', 'CORP', 'PLC',
#         'AND', 'OF', 'THE', 'FOR',
#         'HOME', 'FUND', 'BANK', 'TECH',
#         'ILTD',
#     }
#     parts = s.split()
#     if parts and len(parts[-1]) <= 5 and parts[-1].upper() not in _VALID_SUFFIXES:
#         if re.match(r'^[A-Z]+$', parts[-1], re.IGNORECASE):
#             s = ' '.join(parts[:-1]).strip()

#     return s.strip()


# def _parse_neft_client(narration: str) -> str:
#     """
#     Extract client-like substring from a bank NEFT narration string.
#     Handles NEFT, CMS, SGB, INF, RTGS patterns.
#     Returns '' if not extractable.
#     """
#     if not narration:
#         return ""

#     n = str(narration).strip()
#     n = n.replace("_x000D_\r\n", "").replace("_x000D_\n", "")
#     n = n.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
#     n = re.sub(r"\s+", " ", n).strip()

#     # UPI narrations — reject immediately
#     if re.match(r"^UPI[/\s]", n, re.IGNORECASE):
#         return ""

#     # Pattern 1: NEFT-BANKCODE-CLIENT NAME-...
#     if n.upper().startswith("NEFT"):
#         parts = re.split(r"\s*-\s*", n)
#         if len(parts) >= 3:
#             candidate = parts[2].strip()
#             candidate = re.sub(r"\s+\d+\s*$", "", candidate).strip()
#             candidate = _clean_candidate(candidate)
#             if len(candidate) > 4:
#                 return candidate

#     # Pattern 2: CMS/ REFNO TDS AMOUNT /CLIENT NAME
#     cms = re.search(r"/([A-Z][A-Z\s&.()\-']+?)(?:\s*$)", n, re.IGNORECASE)
#     if cms and n.upper().startswith("CMS"):
#         candidate = _clean_candidate(cms.group(1).strip())
#         if len(candidate) > 4:
#             return candidate

#     # Pattern 4: INF/INFT/.../Inv Paid/CLIENT
#     if n.upper().startswith("INF"):
#         inf = re.search(r'(?:Inv\s+Paid|Invoice\s+Paid)[/\s]+([A-Z][A-Z\s&.()\-\']+)', n, re.IGNORECASE)
#         if inf:
#             candidate = _clean_candidate(inf.group(1).strip())
#             if len(candidate) > 4:
#                 return candidate

#     # Pattern 3: SGB/.../CLIENT NAME/FT
#     if not n.upper().startswith("INF"):
#         sgb = re.findall(r"/([A-Z][A-Z\s&.()\-']+?)(?:/|$)", n, re.IGNORECASE)
#         for candidate in reversed(sgb):
#             candidate = _clean_candidate(candidate.strip())
#             if len(candidate) > 4 and not re.match(r"^FT\s*$", candidate, re.I):
#                 return candidate

#     # Pattern 5: RTGS CR-BANKREF -CLIENT NAME -...
#     if "RTGS" in n.upper():
#         parts = re.split(r"\s*-\s*", n)
#         if len(parts) >= 3:
#             candidate = _clean_candidate(parts[2].strip())
#             if len(candidate) > 4:
#                 return candidate

#     return ""


# def _is_non_client(candidate: str) -> bool:
#     """
#     Returns True if the extracted candidate is NOT a real client.
#     Gates bank internals, individual UPI, routing artifacts, co-op banks, etc.
#     """
#     if not candidate:
#         return False
#     c = candidate.strip().upper()

#     _NON_CLIENT_PATTERNS = [
#         r'^CFD OGL AP',
#         r'^IDFC ACCOUNTS PAYABLE',
#         r'^AP PAYMENT CONTROL',
#         r'^OUTWARD NEFT SETTLEMENT',
#         r'^HO CURRENT ACCOUNT',
#         r'^RTGS OUTWARD POOL',
#         r'^DCB NEFT BRANCH',
#         r'^KVB EXPENSES',
#         r'^FINANCE MGMT VENDOR',
#         r'^AUSFB\s*$',
#         r'^UPI\b',
#         r'^PAYMENT FROM PHONEPE',
#         r'^SENT USING PAYTM',
#         r'^VALUATION FEE\b',
#         r'^REPCO\s*VALUAT',
#         r'^[A-Z]{4}\d{7,}',
#         r'^CMS\d+$',
#         r'^\d{15,}$',
#         r'CO\.?\s*-?\s*OP\s+BANK',
#         r'^SARASWAT\b',
#         r'^CENTRAL BANK',
#         r'^BANK OF BARODA\b',
#         r'^PUNJAB NATIONAL\b',
#         r'^STATE BANK',
#         r'^HDFC BANK\b',
#         r'^SOUTH INDIAN\b',
#         r'^INDIAN OVERSEAS\b',
#         r'^UJJIVAN SMALL\b',
#         r'^JANABANK\b',
#         r'^PAYMENT\s*$',
#     ]

#     for pat in _NON_CLIENT_PATTERNS:
#         if re.search(pat, c, re.IGNORECASE):
#             return True
#     return False


# # ══════════════════════════════════════════════════════════════════════════════
# #  CLIENT NAME HELPERS (SINGLE SOURCE)
# # ══════════════════════════════════════════════════════════════════════════════

# def _extract_branch(party_name: str) -> str:
#     """Extract branch suffix: 'CHOICE FINSERV PVT. LTD._GURGAON' → 'GURGAON'"""
#     if not party_name:
#         return ""
#     s = str(party_name).strip()
#     for sep in ("_", "-"):
#         if sep in s:
#             return s.split(sep)[-1].strip().upper()
#     return ""


# def _extract_client_base(party_name: str) -> str:
#     """
#     Extract base client name without branch suffix.
#     'CHOICE FINSERV PVT. LTD._GURGAON' → 'CHOICE FINSERV PVT. LTD.'

#     SINGLE SOURCE — replaces _base() in old presentation.py and
#     _extract_client_base() in old reconciler.py.
#     """
#     if not party_name:
#         return ""
#     s = str(party_name).strip()
#     for sep in ("_", "-"):
#         if sep in s:
#             return s.rsplit(sep, 1)[0].strip().upper()
#     return s.upper()


# # ══════════════════════════════════════════════════════════════════════════════
# #  CLIENT RESOLVER
# # ══════════════════════════════════════════════════════════════════════════════

# class ClientResolver:
#     """
#     Resolves a raw text string (receipt Particulars or suspense Narration)
#     to a standardised debtors Party's Name.

#     Matching chain:
#         1. KB match_text() → Official_Name → bridge → PNM std name
#         2. Direct PNM standardise_party_name() on raw text
#         3. Branch-aware fuzzy against all debtors names in candidate pool
#         4. Fail → return None, confidence=0
#     """

#     def __init__(self, kb_path: str, debtors_names: list,
#                  validator: Validator = None, kb_bridge: dict = None):
#         self.validator     = validator
#         self.kb_bridge     = kb_bridge or {}
#         self.debtors_names = [str(n).strip() for n in debtors_names if n]

#         if self.validator is None and kb_path and Path(kb_path).exists():
#             self.validator = Validator(kb_path)
#             self.kb_bridge = _build_kb_bridge(kb_path)

#     def resolve(self, raw_text: str) -> dict:
#         """
#         Resolve raw_text to a debtors Party's Name.
#         Returns dict with resolved_name, confidence, method, raw_text, suggestion.
#         """
#         result = {
#             "resolved_name": None,
#             "confidence":    0.0,
#             "method":        "UNRESOLVED",
#             "raw_text":      raw_text,
#         }

#         if not raw_text or pd.isna(raw_text):
#             return result

#         text = str(raw_text).strip()

#         # Pass 1: KB match
#         if self.validator:
#             kb_result = self.validator.match_text(text)
#             if kb_result["Client_ID"]:
#                 official = kb_result["Official_Name"]

#                 bridged = self.kb_bridge.get(official.upper())
#                 if bridged and bridged in self.debtors_names:
#                     return {
#                         "resolved_name": bridged,
#                         "confidence":    kb_result["Match_Score"],
#                         "method":        f"KB_{kb_result['Match_Method']}_BRIDGE",
#                         "raw_text":      text,
#                     }

#                 for dname in self.debtors_names:
#                     base_d = _extract_client_base(dname)
#                     base_o = _extract_client_base(official)
#                     if base_d and base_o and base_d == base_o:
#                         return {
#                             "resolved_name": dname,
#                             "confidence":    kb_result["Match_Score"] * 0.95,
#                             "method":        f"KB_{kb_result['Match_Method']}_BASE",
#                             "raw_text":      text,
#                         }

#         # Pass 2: Direct PNM standardisation
#         std = standardise_party_name(text)
#         if std and std != text:
#             for dname in self.debtors_names:
#                 base_d = _extract_client_base(dname)
#                 base_s = _extract_client_base(std)
#                 if base_d and base_s and base_d == base_s:
#                     return {
#                         "resolved_name": dname,
#                         "confidence":    0.90,
#                         "method":        "PNM_DIRECT",
#                         "raw_text":      text,
#                     }

#         # Pass 3: Branch-aware fuzzy with dynamic threshold
#         text_branch = _extract_branch(text)
#         text_base   = _extract_client_base(text)
#         tb_norm     = _normalise_for_fuzzy(text_base)
#         tb_words    = tb_norm.split()
#         n_words     = len(tb_words)
#         n_chars     = len(text_base.strip())

#         if n_chars <= 10:
#             dyn_threshold = 0.92
#         elif n_chars <= 22:
#             dyn_threshold = 0.75 if n_words >= 3 else 0.88
#         else:
#             dyn_threshold = BRANCH_FUZZY_THRESHOLD

#         all_db_norms = {
#             dname: _normalise_for_fuzzy(_extract_client_base(dname))
#             for dname in self.debtors_names
#         }

#         # Special path: 2-word candidates — unique substr only
#         if n_words == 2:
#             substr_hits = [
#                 dname for dname, db_norm in all_db_norms.items()
#                 if tb_norm in db_norm or db_norm in tb_norm
#             ]
#             if len(substr_hits) == 1:
#                 best_name  = substr_hits[0]
#                 best_score = _fuzzy(text_base, _extract_client_base(best_name))
#                 d_branch = _extract_branch(best_name)
#                 if text_branch and d_branch and text_branch != d_branch:
#                     best_name = None
#                     best_score = 0.0
#             else:
#                 best_name  = None
#                 best_score = 0.0

#             if best_name:
#                 return {
#                     "resolved_name": best_name,
#                     "confidence":    round(best_score, 4),
#                     "method":        "FUZZY_BRANCH_AWARE",
#                     "raw_text":      text,
#                 }

#         else:
#             # Standard path: 3+ word and long candidates
#             best_score = 0.0
#             best_name  = None

#             for dname in self.debtors_names:
#                 d_base   = _extract_client_base(dname)
#                 d_branch = _extract_branch(dname)
#                 db_norm  = all_db_norms[dname]

#                 base_score = _fuzzy(text_base, d_base)

#                 if n_words >= 3 and (tb_norm in db_norm or db_norm in tb_norm):
#                     base_score = max(base_score, 0.90)

#                 if base_score < dyn_threshold:
#                     continue

#                 if text_branch and d_branch:
#                     if text_branch != d_branch:
#                         continue
#                     branch_bonus = 0.05
#                 else:
#                     branch_bonus = 0.0

#                 score = min(base_score + branch_bonus, 1.0)
#                 if score > best_score:
#                     best_score = score
#                     best_name  = dname

#             if best_name and best_score >= dyn_threshold:
#                 return {
#                     "resolved_name": best_name,
#                     "confidence":    round(best_score, 4),
#                     "method":        "FUZZY_BRANCH_AWARE",
#                     "raw_text":      text,
#                 }

#         # Pass 4: Fail
#         suggestion = self._suggest_candidate(text)
#         return {
#             "resolved_name": None,
#             "confidence":    0.0,
#             "method":        "UNRESOLVED",
#             "raw_text":      text,
#             "suggestion":    suggestion,
#         }

#     def _suggest_candidate(self, text: str) -> Optional[str]:
#         """Best-effort candidate suggestion for KB Gaps sheet."""
#         text_up = text.upper()
#         best_len = 0
#         best     = None

#         for dname in self.debtors_names:
#             base = _extract_client_base(dname)
#             if len(base) > 4 and base in text_up:
#                 if len(base) > best_len:
#                     best_len = len(base)
#                     best     = dname

#         return best
















"""
backend/reconciler/resolver.py — Client resolution chain.
================================================================================
Responsibility: answer "Who is this payment from?"

Contains the full resolution pipeline:
  1. Validator — Knowledge Bank keyword/fuzzy matching
  2. KB Bridge — maps KB official_name → PNM standardised name
  3. Narration Parser — extracts client name from NEFT/CMS/SGB/INF/RTGS strings
  4. ClientResolver — 4-pass chain: KB → PNM → Fuzzy → Fail

Does NOT handle: bill matching, FIFO application, reconciliation mechanics.
Those belong in engine.py.
================================================================================
"""

import re
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
from difflib import SequenceMatcher

from backend.config import (
    FUZZY_MATCH_THRESHOLD,
    BRANCH_FUZZY_THRESHOLD,
)
from backend.common import (
    _normalise, _normalise_for_fuzzy, _normalise_base, _fuzzy,
    ResolutionResult, GapReason, UnresolvableReason,
)
from backend.parsers.debtors import standardise_party_name, extract_ref_prefix


# ══════════════════════════════════════════════════════════════════════════════
#  KNOWLEDGE BANK — VALIDATOR
# ══════════════════════════════════════════════════════════════════════════════

class Validator:
    """
    Matches client names/narrations against the knowledge bank.

    Usage:
        v = Validator("path/to/master_client_knowledge_base.json")
        result = v.match_text("SUNDARAM HOME FINANCE")
    """

    FUZZY_THRESHOLD = FUZZY_MATCH_THRESHOLD

    def __init__(self, knowledge_bank_path: str):
        self.clients      = []
        self.keyword_map  = {}
        self._load(knowledge_bank_path)

    def _load(self, path: str):
        """Load knowledge bank from JSON and build keyword lookup map."""
        fp = Path(path)
        if not fp.exists():
            print(f"⚠  Knowledge bank not found: {path}")
            return

        with open(fp, "r") as f:
            self.clients = json.load(f)

        for client in self.clients:
            for kw in client.get("keywords", []):
                norm = _normalise(kw)
                self.keyword_map[norm] = {
                    "id":            client["id"],
                    "official_name": client["official_name"],
                }

        print(f"✓  Knowledge bank loaded: {len(self.clients)} clients, {len(self.keyword_map)} keywords")

    def match_text(self, text: str) -> dict:
        """
        Try to match a single text string against the knowledge bank.
        3-pass: exact → substring → fuzzy.
        """
        unmatched = {
            "Client_ID":     None,
            "Official_Name": None,
            "Match_Method":  "UNMATCHED",
            "Match_Score":   0.0,
        }

        if not text or pd.isna(text):
            return unmatched

        norm_text = _normalise(text)

        # Pass 1: Exact match
        if norm_text in self.keyword_map:
            hit = self.keyword_map[norm_text]
            return {
                "Client_ID":     hit["id"],
                "Official_Name": hit["official_name"],
                "Match_Method":  "EXACT",
                "Match_Score":   1.0,
            }

        # Pass 2: Substring match
        best_sub = None
        best_sub_score = 0

        for kw, hit in self.keyword_map.items():
            if kw in norm_text or norm_text in kw:
                pos = norm_text.find(kw)
                length_score = len(kw)
                if pos >= 0:
                    position_factor = 1.0 + 0.2 * max(0, 1.0 - pos / max(len(norm_text), 1))
                else:
                    position_factor = 1.0
                score = length_score * position_factor
                if score > best_sub_score:
                    best_sub_score = score
                    best_sub = hit

        if best_sub:
            return {
                "Client_ID":     best_sub["id"],
                "Official_Name": best_sub["official_name"],
                "Match_Method":  "SUBSTRING",
                "Match_Score":   0.95,
            }

        # Pass 3: Fuzzy match
        best_score = 0.0
        best_hit   = None

        text_words = set(norm_text.split())
        for kw, hit in self.keyword_map.items():
            kw_words = set(kw.split())
            if not text_words & kw_words:
                continue

            score = _fuzzy(norm_text, kw)
            if score > best_score:
                best_score = score
                best_hit   = hit

        if best_score >= self.FUZZY_THRESHOLD and best_hit:
            return {
                "Client_ID":     best_hit["id"],
                "Official_Name": best_hit["official_name"],
                "Match_Method":  "FUZZY",
                "Match_Score":   round(best_score, 4),
            }

        return unmatched

    def validate(self, df: pd.DataFrame, text_col: str) -> pd.DataFrame:
        """Run knowledge bank matching on every row of a DataFrame."""
        if df.empty:
            return df

        if text_col not in df.columns:
            print(f"  ⚠  Column '{text_col}' not found — skipping validation")
            return df

        print(f"\n{'='*60}")
        print(f"VALIDATING ({len(df)} rows, source: '{text_col}')")
        print(f"{'='*60}")

        results = df[text_col].apply(self.match_text)
        result_df = pd.DataFrame(results.tolist())

        result_df["KB_Matched"] = result_df["Client_ID"].notna()

        drop_cols = ["Client_ID", "Official_Name", "Match_Method",
                     "Match_Score", "KB_Matched"]
        base = df.drop(columns=[c for c in drop_cols if c in df.columns])
        out  = pd.concat([base.reset_index(drop=True),
                          result_df.reset_index(drop=True)], axis=1)

        matched   = result_df["KB_Matched"].sum()
        unmatched = len(result_df) - matched
        methods   = result_df[result_df["KB_Matched"]]["Match_Method"] \
                        .value_counts().to_dict()

        print(f"\n  Matched   : {matched:,}  ({matched/len(df)*100:.1f}%)")
        print(f"  Unmatched : {unmatched:,}  ({unmatched/len(df)*100:.1f}%)")
        if methods:
            for method, count in methods.items():
                print(f"    {method:<12s}: {count:,}")

        return out


# ══════════════════════════════════════════════════════════════════════════════
#  KB BRIDGE BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _build_kb_bridge(kb_path: str) -> dict:
    """Build bridge: KB official_name → PNM standardised name."""
    bridge = {}
    try:
        with open(kb_path, "r", encoding="utf-8") as f:
            kb_clients = json.load(f)
    except Exception:
        return bridge

    for client in kb_clients:
        official = client.get("official_name", "")
        keywords = client.get("keywords", [])

        std = standardise_party_name(official)
        if std and std.upper() != official.upper():
            bridge[official.upper()] = std

        for kw in keywords:
            std_kw = standardise_party_name(kw)
            if std_kw and std_kw.upper() != kw.upper():
                if official.upper() not in bridge:
                    bridge[official.upper()] = std_kw
                break

    return bridge


# ══════════════════════════════════════════════════════════════════════════════
#  NARRATION PARSER
# ══════════════════════════════════════════════════════════════════════════════

def _clean_candidate(candidate: str) -> str:
    """
    Post-extraction cleanup applied to every candidate before it is returned
    from _parse_neft_client. Handles trailing operational suffixes and
    alpha-numeric bleed.
    """
    if not candidate:
        return candidate

    s = candidate.strip()

    # Step 1: Strip trailing operational suffixes (longest first)
    _OP_SUFFIXES = [
        'EXPENSES ACCOUNT', 'EXPENSE ACCOUNT',
        'TREASURY ACCOUNT', 'TREASURY AC',
        'CURRENT ACCOUNT',  'SAVINGS ACCOUNT',
        'DISBURSEMENT AC',  'DISBURSEMENT',
        'PAYMENT ACCOUNT',  'COLL JAIPUR',
        'EXPENSE AC',
    ]
    su = s.upper()
    for suffix in _OP_SUFFIXES:
        if su.endswith(suffix):
            s = s[:len(s) - len(suffix)].strip().rstrip('-').strip()
            break

    # Step 2: Strip trailing alpha-numeric bleed
    s = re.sub(r'\s+[A-Z]{0,5}\d+[A-Z0-9]*\s*$', '', s, flags=re.IGNORECASE).strip()

    # Pure-alpha truncation stub
    _VALID_SUFFIXES = {
        'PVT', 'LTD', 'LLP', 'INC', 'CO', 'CORP', 'PLC',
        'AND', 'OF', 'THE', 'FOR',
        'HOME', 'FUND', 'BANK', 'TECH',
        'ILTD',
    }
    parts = s.split()
    if parts and len(parts[-1]) <= 5 and parts[-1].upper() not in _VALID_SUFFIXES:
        if re.match(r'^[A-Z]+$', parts[-1], re.IGNORECASE):
            s = ' '.join(parts[:-1]).strip()

    return s.strip()


def _parse_neft_client(narration: str) -> str:
    """
    Extract client-like substring from a bank NEFT narration string.
    Handles NEFT, CMS, SGB, INF, RTGS patterns.
    Returns '' if not extractable.
    """
    if not narration:
        return ""

    n = str(narration).strip()
    n = n.replace("_x000D_\r\n", "").replace("_x000D_\n", "")
    n = n.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    n = re.sub(r"\s+", " ", n).strip()

    # UPI narrations — reject immediately
    if re.match(r"^UPI[/\s]", n, re.IGNORECASE):
        return ""

    # Pattern 1: NEFT-BANKCODE-CLIENT NAME-...
    if n.upper().startswith("NEFT"):
        parts = re.split(r"\s*-\s*", n)
        if len(parts) >= 3:
            candidate = parts[2].strip()
            candidate = re.sub(r"\s+\d+\s*$", "", candidate).strip()
            candidate = _clean_candidate(candidate)
            if len(candidate) > 4:
                return candidate

    # Pattern 2: CMS/ REFNO TDS AMOUNT /CLIENT NAME
    cms = re.search(r"/([A-Z][A-Z\s&.()\-']+?)(?:\s*$)", n, re.IGNORECASE)
    if cms and n.upper().startswith("CMS"):
        candidate = _clean_candidate(cms.group(1).strip())
        if len(candidate) > 4:
            return candidate

    # Pattern 4: INF/INFT/.../Inv Paid/CLIENT
    if n.upper().startswith("INF"):
        inf = re.search(r'(?:Inv\s+Paid|Invoice\s+Paid)[/\s]+([A-Z][A-Z\s&.()\-\']+)', n, re.IGNORECASE)
        if inf:
            candidate = _clean_candidate(inf.group(1).strip())
            if len(candidate) > 4:
                return candidate

    # Pattern 3: SGB/.../CLIENT NAME/FT
    if not n.upper().startswith("INF"):
        sgb = re.findall(r"/([A-Z][A-Z\s&.()\-']+?)(?:/|$)", n, re.IGNORECASE)
        for candidate in reversed(sgb):
            candidate = _clean_candidate(candidate.strip())
            if len(candidate) > 4 and not re.match(r"^FT\s*$", candidate, re.I):
                return candidate

    # Pattern 5: RTGS CR-BANKREF -CLIENT NAME -...
    if "RTGS" in n.upper():
        parts = re.split(r"\s*-\s*", n)
        if len(parts) >= 3:
            candidate = _clean_candidate(parts[2].strip())
            if len(candidate) > 4:
                return candidate

    return ""


def _is_non_client(candidate: str) -> bool:
    """
    Returns True if the extracted candidate is NOT a real client.
    Gates bank internals, individual UPI, routing artifacts, co-op banks, etc.
    """
    if not candidate:
        return False
    c = candidate.strip().upper()

    _NON_CLIENT_PATTERNS = [
        r'^CFD OGL AP',
        r'^IDFC ACCOUNTS PAYABLE',
        r'^AP PAYMENT CONTROL',
        r'^OUTWARD NEFT SETTLEMENT',
        r'^HO CURRENT ACCOUNT',
        r'^RTGS OUTWARD POOL',
        r'^DCB NEFT BRANCH',
        r'^KVB EXPENSES',
        r'^FINANCE MGMT VENDOR',
        r'^AUSFB\s*$',
        r'^UPI\b',
        r'^PAYMENT FROM PHONEPE',
        r'^SENT USING PAYTM',
        r'^VALUATION FEE\b',
        r'^REPCO\s*VALUAT',
        r'^[A-Z]{4}\d{7,}',
        r'^CMS\d+$',
        r'^\d{15,}$',
        r'CO\.?\s*-?\s*OP\s+BANK',
        r'^SARASWAT\b',
        r'^CENTRAL BANK',
        r'^BANK OF BARODA\b',
        r'^PUNJAB NATIONAL\b',
        r'^STATE BANK',
        r'^HDFC BANK\b',
        r'^SOUTH INDIAN\b',
        r'^INDIAN OVERSEAS\b',
        r'^UJJIVAN SMALL\b',
        r'^JANABANK\b',
        r'^PAYMENT\s*$',
    ]

    for pat in _NON_CLIENT_PATTERNS:
        if re.search(pat, c, re.IGNORECASE):
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  CLIENT NAME HELPERS (SINGLE SOURCE)
# ══════════════════════════════════════════════════════════════════════════════

def _extract_branch(party_name: str) -> str:
    """Extract branch suffix: 'CHOICE FINSERV PVT. LTD._GURGAON' → 'GURGAON'"""
    if not party_name:
        return ""
    s = str(party_name).strip()
    for sep in ("_", "-"):
        if sep in s:
            return s.split(sep)[-1].strip().upper()
    return ""


def _extract_client_base(party_name: str) -> str:
    """
    Extract base client name without branch suffix.
    'CHOICE FINSERV PVT. LTD._GURGAON' → 'CHOICE FINSERV PVT. LTD.'

    SINGLE SOURCE — replaces _base() in old presentation.py and
    _extract_client_base() in old reconciler.py.
    """
    if not party_name:
        return ""
    s = str(party_name).strip()
    for sep in ("_", "-"):
        if sep in s:
            return s.rsplit(sep, 1)[0].strip().upper()
    return s.upper()


# ══════════════════════════════════════════════════════════════════════════════
#  CLIENT RESOLVER
# ══════════════════════════════════════════════════════════════════════════════

class ClientResolver:
    """
    Resolves a raw text string (receipt Particulars or suspense Narration)
    to a standardised debtors Party's Name.

    Matching chain:
        1. KB match_text() → Official_Name → bridge → PNM std name
        2. Direct PNM standardise_party_name() on raw text
        3. Branch-aware fuzzy against all debtors names in candidate pool
        4. Fail → return None, confidence=0
    """

    def __init__(self, kb_path: str, debtors_names: list,
                 validator: Validator = None, kb_bridge: dict = None):
        self.validator     = validator
        self.kb_bridge     = kb_bridge or {}
        self.debtors_names = [str(n).strip() for n in debtors_names if n]

        if self.validator is None and kb_path and Path(kb_path).exists():
            self.validator = Validator(kb_path)
            self.kb_bridge = _build_kb_bridge(kb_path)

    def resolve(self, raw_text: str) -> dict:
        """
        Resolve raw_text to a debtors Party's Name.
        Returns dict with resolved_name, confidence, method, raw_text, suggestion.
        """
        result = ResolutionResult(
            resolved_name=None, confidence=0.0,
            method="UNRESOLVED", raw_text=raw_text,
        )

        if not raw_text or pd.isna(raw_text):
            return result

        text = str(raw_text).strip()

        # Pass 1: KB match
        if self.validator:
            kb_result = self.validator.match_text(text)
            if kb_result["Client_ID"]:
                official = kb_result["Official_Name"]

                bridged = self.kb_bridge.get(official.upper())
                if bridged and bridged in self.debtors_names:
                    return ResolutionResult(
                        resolved_name=bridged,
                        confidence=kb_result["Match_Score"],
                        method=f"KB_{kb_result['Match_Method']}_BRIDGE",
                        raw_text=text,
                    )

                for dname in self.debtors_names:
                    base_d = _extract_client_base(dname)
                    base_o = _extract_client_base(official)
                    if base_d and base_o and base_d == base_o:
                        return ResolutionResult(
                            resolved_name=dname,
                            confidence=kb_result["Match_Score"] * 0.95,
                            method=f"KB_{kb_result['Match_Method']}_BASE",
                            raw_text=text,
                        )

        # Pass 2: Direct PNM standardisation
        std = standardise_party_name(text)
        if std and std != text:
            for dname in self.debtors_names:
                base_d = _extract_client_base(dname)
                base_s = _extract_client_base(std)
                if base_d and base_s and base_d == base_s:
                    return ResolutionResult(
                        resolved_name=dname,
                        confidence=0.90,
                        method="PNM_DIRECT",
                        raw_text=text,
                    )

        # Pass 3: Branch-aware fuzzy with dynamic threshold
        text_branch = _extract_branch(text)
        text_base   = _extract_client_base(text)
        tb_norm     = _normalise_for_fuzzy(text_base)
        tb_words    = tb_norm.split()
        n_words     = len(tb_words)
        n_chars     = len(text_base.strip())

        if n_chars <= 10:
            dyn_threshold = 0.92
        elif n_chars <= 22:
            dyn_threshold = 0.75 if n_words >= 3 else 0.88
        else:
            dyn_threshold = BRANCH_FUZZY_THRESHOLD

        all_db_norms = {
            dname: _normalise_for_fuzzy(_extract_client_base(dname))
            for dname in self.debtors_names
        }

        # Special path: 2-word candidates — unique substr only
        if n_words == 2:
            substr_hits = [
                dname for dname, db_norm in all_db_norms.items()
                if tb_norm in db_norm or db_norm in tb_norm
            ]
            if len(substr_hits) == 1:
                best_name  = substr_hits[0]
                best_score = _fuzzy(text_base, _extract_client_base(best_name))
                d_branch = _extract_branch(best_name)
                if text_branch and d_branch and text_branch != d_branch:
                    best_name = None
                    best_score = 0.0
            else:
                best_name  = None
                best_score = 0.0

            if best_name:
                return ResolutionResult(
                    resolved_name=best_name,
                    confidence=round(best_score, 4),
                    method="FUZZY_BRANCH_AWARE",
                    raw_text=text,
                )

        else:
            # Standard path: 3+ word and long candidates
            best_score = 0.0
            best_name  = None

            for dname in self.debtors_names:
                d_base   = _extract_client_base(dname)
                d_branch = _extract_branch(dname)
                db_norm  = all_db_norms[dname]

                base_score = _fuzzy(text_base, d_base)

                if n_words >= 3 and (tb_norm in db_norm or db_norm in tb_norm):
                    base_score = max(base_score, 0.90)

                if base_score < dyn_threshold:
                    continue

                if text_branch and d_branch:
                    if text_branch != d_branch:
                        continue
                    branch_bonus = 0.05
                else:
                    branch_bonus = 0.0

                score = min(base_score + branch_bonus, 1.0)
                if score > best_score:
                    best_score = score
                    best_name  = dname

            if best_name and best_score >= dyn_threshold:
                return ResolutionResult(
                    resolved_name=best_name,
                    confidence=round(best_score, 4),
                    method="FUZZY_BRANCH_AWARE",
                    raw_text=text,
                )

        # Pass 4: Fail
        suggestion = self._suggest_candidate(text)
        return ResolutionResult(
            resolved_name=None, confidence=0.0,
            method="UNRESOLVED", raw_text=text,
            suggestion=suggestion,
        )

    def _suggest_candidate(self, text: str) -> Optional[str]:
        """Best-effort candidate suggestion for KB Gaps sheet."""
        text_up = text.upper()
        best_len = 0
        best     = None

        for dname in self.debtors_names:
            base = _extract_client_base(dname)
            if len(base) > 4 and base in text_up:
                if len(base) > best_len:
                    best_len = len(base)
                    best     = dname

        return best