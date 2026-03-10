"""
backend/common.py — Shared utilities, enums, and dataclasses.
================================================================================
This module is a leaf dependency: imported by every other backend module,
imports nothing project-internal (only config.py constants).

Consolidates duplicated utilities from the old codebase into SINGLE SOURCES:
  - _clean_tally_text    (was in debtors_logic_test + receipts_logic_test)
  - _fuzzy               (was in validator + reconciler with different normalisers)
  - _is_skip_particular  (was in receipts_logic_test + suspense_logic_test with
                           different skip-sets — unified to superset)
  - _find_header_row     (was in receipts + suspense with divergent signatures)
  - _get_col_indices     (was in receipts only, now shared)
  - _parse_date          (was in debtors_logic_test)
  - _cell_to_str         (was in debtors_logic_test)

Does NOT contain any domain logic (parsing, matching, reconciliation).
================================================================================
"""

import re
from enum import Enum
from datetime import datetime, date
from difflib import SequenceMatcher
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd


# ══════════════════════════════════════════════════════════════════════════════
#  TEXT UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _clean_tally_text(s) -> str:
    """
    Remove Tally Excel export artefacts that corrupt client name extraction.

    SINGLE SOURCE — replaces identical copies in old debtors_logic_test.py
    and receipts_logic_test.py.

    Tally encodes Windows carriage returns as '_x000D_' followed by a newline
    when exporting to .xlsx. This splits client names mid-word, e.g.:
        'CHOICE FINSERV PRI_x000D_\\nVATE LIMITED'  -> 'CHOICE FINSERV PRIVATE LIMITED'
        'NEFT-AUBLH0322318047_x000D_\\n5-CLIENT'     -> 'NEFT-AUBLH03223180475-CLIENT'
    """
    if not s or not isinstance(s, str):
        return s or ""
    s = s.replace("_x000D_\r\n", "").replace("_x000D_\n", "")
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _normalise(text: str) -> str:
    """Uppercase, strip, collapse whitespace."""
    return " ".join(str(text).upper().strip().split())


def _normalise_for_fuzzy(s: str) -> str:
    """Normalise string for fuzzy comparison.
    - Uppercase
    - Remove parentheses and their contents variant: (INDIA) → INDIA
    - Collapse multiple spaces
    - Strip punctuation noise
    """
    s = s.upper().strip()
    s = re.sub(r'[()]', ' ', s)          # remove parentheses
    s = re.sub(r'[.,]', '', s)           # remove dots and commas
    s = re.sub(r'\s+', ' ', s).strip()   # collapse spaces
    return s


def _normalise_base(base: str) -> str:
    """
    Normalise terminal legal-form suffix so that LTD / LTD. / LIMITED all
    compare equal when matching resolver output against debtors _base_name.

    Does NOT collapse PVT/PRIVATE — those denote different company classes.
    """
    b = base.strip().rstrip(".")                       # "LTD." → "LTD"
    b = re.sub(r"\bLTD\b", "LIMITED", b, flags=re.IGNORECASE)
    b = re.sub(r"\bLIM\b", "LIMITED", b, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", b).strip().upper()


def _fuzzy(a: str, b: str, normaliser=None) -> float:
    """
    Sequence match ratio between two strings.

    SINGLE SOURCE — replaces separate implementations in old validator.py
    and reconciler.py. The `normaliser` parameter handles the difference:
      - Validator calls: _fuzzy(a, b)  (no normalisation, caller pre-normalised)
      - Reconciler calls: _fuzzy(a, b, normaliser=_normalise_for_fuzzy)

    Args:
        a, b: strings to compare
        normaliser: optional callable to normalise both strings before comparison.
                    If None, strings are compared as-is.
    """
    if not a or not b:
        return 0.0
    if normaliser:
        a = normaliser(a)
        b = normaliser(b)
    return SequenceMatcher(None, a, b).ratio()


# ══════════════════════════════════════════════════════════════════════════════
#  DATE UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

DATE_FORMATS = [
    "%d-%b-%y",   # 1-Oct-25
    "%d-%b-%Y",   # 1-Oct-2025
    "%d/%m/%Y",   # 01/10/2025
    "%Y-%m-%d",   # 2025-10-01
]


def _parse_date(val):
    """Parse a date value from Excel (datetime object or string)."""
    if isinstance(val, (datetime, date)):
        return val.date() if isinstance(val, datetime) else val
    if isinstance(val, str):
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(val.strip(), fmt).date()
            except ValueError:
                continue
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _cell_to_str(cell):
    """Safe cell → lowercase string."""
    return str(cell).strip().lower() if cell is not None else ""


# Unified skip-set — superset of both old receipts and suspense versions.
# Old receipts had: {"", "OPENING BALANCE", "CLOSING BALANCE"}
# Old suspense had: {"", "#N/A", "CLOSING BALANCE", "OPENING BALANCE"}
# The difference was likely a bug. This is the union.
_SKIP_PARTICULARS = {"", "#N/A", "CLOSING BALANCE", "OPENING BALANCE"}


def _is_skip_particular(val):
    """
    Returns True if a Particulars cell should be skipped during parsing.

    SINGLE SOURCE — replaces divergent implementations in old
    receipts_logic_test.py and suspense_logic_test.py.
    """
    if val is None:
        return True
    s = str(val).strip().upper()
    if s in _SKIP_PARTICULARS:
        return True
    if "OPENING BALANCE" in s:
        return True
    return False


def _find_header_row(ws, max_rows=15):
    """
    Find the row number (1-based) of the header row containing 'Date'.

    SINGLE SOURCE — replaces divergent implementations:
      - Old receipts version returned (row_number, col_count)
      - Old suspense version returned just row_number

    Unified signature returns (row_number, row_length) or (None, 7).
    Callers that don't need row_length can ignore the second value.
    """
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=max_rows, values_only=True), 1):
        if any(str(v).strip() == "Date" for v in row if v is not None):
            return i, len(row)
    return None, 7


def _get_col_indices(row_len):
    """
    Return column indices based on detected row length.
    7-col format: standard Tally layout.
    9-col format (AU BANK): two blank spacers at idx 3-4 push Vch Type to idx 5.
    """
    if row_len >= 9:
        return dict(DATE=0, PARTICULAR=2, VCH_TYPE=5, VCH_NO=6, DEBIT=7, CREDIT=8)
    else:
        return dict(DATE=0, PARTICULAR=2, VCH_TYPE=3, VCH_NO=4, DEBIT=5, CREDIT=6)


# ══════════════════════════════════════════════════════════════════════════════
#  ENUMS — replace magic string constants (R4)
# ══════════════════════════════════════════════════════════════════════════════

class GapReason(str, Enum):
    """Reason codes for KB Gaps entries."""
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    UNKNOWN_CLIENT = "UNKNOWN_CLIENT"
    NO_OPEN_BILLS = "NO_OPEN_BILLS"


class UnresolvableReason(str, Enum):
    """Reason codes for completely unresolvable entries."""
    NARRATION_UNPARSEABLE = "NARRATION_UNPARSEABLE"
    NON_CLIENT_NARRATION = "NON_CLIENT_NARRATION"
    CPV_CPD_UNDETERMINED = "CPV_CPD_UNDETERMINED"
    NO_NARRATION = "NO_NARRATION"


class BillStatus(str, Enum):
    """Status values for debtors bills."""
    OPEN = "OPEN"
    CLEARED = "CLEARED"
    PARTIALLY_CLEARED = "PARTIALLY_CLEARED"


class SuspenseStatus(str, Enum):
    """Status values for suspense entries."""
    OPEN = "OPEN"
    FULLY_APPLIED = "FULLY_APPLIED"
    PARTIALLY_APPLIED = "PARTIALLY_APPLIED"


# ══════════════════════════════════════════════════════════════════════════════
#  DATACLASSES — inter-module contracts (R8)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ResolutionResult:
    """Result of client name resolution (resolver → engine contract)."""
    resolved_name: Optional[str]
    confidence: float
    method: str
    raw_text: str
    suggestion: Optional[str] = None


@dataclass
class ApplicationEvent:
    """Single bill knock-off event (engine → statement row)."""
    vertical: str
    client: str
    bill_ref: str
    bill_date: str
    bill_amount: float
    cleared_by: str
    cleared_ref: str
    cleared_date: str
    cleared_amount: float
    remaining_after: float
    confidence: float
    method: str
    narration: str


@dataclass
class ReconciliationOutput:
    """Complete output of a single-vertical reconciliation run."""
    statement: pd.DataFrame
    updated_debtors: pd.DataFrame
    updated_suspense: pd.DataFrame
    kb_gaps: pd.DataFrame
    advance_payments: pd.DataFrame
    unresolvable: pd.DataFrame
    summary: dict