"""
backend/output/presentation.py — Display / presentation layer.
================================================================================
All rename maps, column visibility lists, value translators, and computed
column helpers live here. The reconciler engine emits internal codes;
this module translates them to human-readable display values.

Three layers:
    Layer 1 — Value rename maps   (Method codes, Reason codes, Status values)
    Layer 2 — Column visibility   (STREAMLIT_COLS / EXCEL_COLS per sheet)
    Layer 3 — Computed columns    (Nearest Bill Amount on KB Gaps)
================================================================================
"""

import re
import pandas as pd

from backend.reconciler.resolver import _extract_client_base
from backend.common import (
    BillStatus, SuspenseStatus, GapReason, UnresolvableReason,
)

INR = "\u20b9"   # ₹


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 1 — VALUE RENAME MAPS
# ══════════════════════════════════════════════════════════════════════════════

# ── Match Method ──────────────────────────────────────────────────────────────
# Internal engine codes → plain English for Excel download.
# _AGGREGATED suffix is stripped and replaced with a parenthetical.

_METHOD_BASE_MAP = {
    "KB_EXACT_BASE":       "Knowledge Base — Exact Match",
    "KB_EXACT_BRIDGE":     "Knowledge Base — Exact Match (Alias)",
    "KB_SUBSTRING_BASE":   "Knowledge Base — Partial Name Match",
    "KB_SUBSTRING_BRIDGE": "Knowledge Base — Alias Match",
    "KB_FUZZY_BASE":       "Knowledge Base — Close Name Match",
    "KB_FUZZY_BRIDGE":     "Knowledge Base — Close Name Match (Alias)",
    "PNM_DIRECT":          "Name Standardisation Match",
    "FUZZY_BRANCH_AWARE":  "Fuzzy Match — Branch Identified",
    "UNRESOLVED":          "Unresolved",
}

def translate_method(raw: str) -> str:
    """
    Translate internal Match_Method code to plain English.
    Handles *_AGGREGATED suffix: strips it and appends '+ Multi-Payment Group'.
    Falls back to the raw string if no match found.
    """
    if not raw or pd.isna(raw):
        return ""
    s = str(raw).strip()
    aggregated = s.endswith("_AGGREGATED")
    base = s[:-len("_AGGREGATED")] if aggregated else s
    label = _METHOD_BASE_MAP.get(base, base)
    if aggregated:
        label += " + Multi-Payment Group"
    return label


# ── Match Confidence → tier label ─────────────────────────────────────────────

def translate_confidence(val) -> str:
    """
    Convert float confidence score to human-readable tier label.
    ≥0.95 → Very High | 0.90–0.95 → High | 0.82–0.90 → Medium | <0.82 → Low (Review Recommended)
    """
    try:
        v = float(val)
        if v >= 0.95: return "Very High"
        if v >= 0.90: return "High"
        if v >= 0.82: return "Medium"
        return "Low (Review Recommended)"
    except Exception:
        return str(val) if val else ""


# ── Status values ─────────────────────────────────────────────────────────────

_DEBTORS_STATUS_MAP = {
    BillStatus.OPEN:               "Open",
    BillStatus.CLEARED:            "Fully Settled",
    BillStatus.PARTIALLY_CLEARED:  "Partly Settled",
}

_SUSPENSE_STATUS_MAP = {
    SuspenseStatus.OPEN:               "Pending",
    SuspenseStatus.FULLY_APPLIED:      "Fully Applied",
    SuspenseStatus.PARTIALLY_APPLIED:  "Partially Applied",
}

def translate_debtors_status(val) -> str:
    """Translate BillStatus enum (or legacy string) to display string."""
    # Direct enum lookup
    if val in _DEBTORS_STATUS_MAP:
        return _DEBTORS_STATUS_MAP[val]
    # Legacy string fallback
    upper = str(val).upper().strip()
    for k, v in _DEBTORS_STATUS_MAP.items():
        if k.value == upper:
            return v
    return str(val) if val else ""

def translate_suspense_status(val) -> str:
    """Translate SuspenseStatus enum (or legacy string) to display string."""
    if val in _SUSPENSE_STATUS_MAP:
        return _SUSPENSE_STATUS_MAP[val]
    upper = str(val).upper().strip()
    for k, v in _SUSPENSE_STATUS_MAP.items():
        if k.value == upper:
            return v
    return str(val) if val else ""


# ── KB Gaps — Action_Required → Gap Reason tag ───────────────────────────────

_GAP_REASON_MAP = {
    GapReason.AMOUNT_MISMATCH: "Amount Mismatch",
    GapReason.UNKNOWN_CLIENT:  "Unknown Client — Add to Register",
    GapReason.NO_OPEN_BILLS:   "No Open Invoice",
}

def translate_gap_reason(val) -> str:
    """Translate GapReason enum (or legacy string) to display string."""
    if val in _GAP_REASON_MAP:
        return _GAP_REASON_MAP[val]
    # Legacy string fallback
    _LEGACY = {
        "Amount does not match any open bill within tolerance": "Amount Mismatch",
        "Add to KB / PNM": "Unknown Client — Add to Register",
        "No open bills found for this client": "No Open Invoice",
    }
    return _LEGACY.get(str(val).strip(), str(val) if val else "")


# ── Unresolvable — Reason → plain English ────────────────────────────────────

_UNRESOLVABLE_REASON_MAP = {
    UnresolvableReason.NARRATION_UNPARSEABLE: "No Client Info in Bank Narration",
    UnresolvableReason.NON_CLIENT_NARRATION:  "Payment from Non-Client",
    UnresolvableReason.CPV_CPD_UNDETERMINED:  "Invoice Type Could Not Be Determined",
    UnresolvableReason.NO_NARRATION:          "No Narration Available",
}

def translate_unresolvable_reason(val) -> str:
    """Translate UnresolvableReason enum (or legacy string) to display string."""
    if val in _UNRESOLVABLE_REASON_MAP:
        return _UNRESOLVABLE_REASON_MAP[val]
    # Legacy string fallback
    _LEGACY = {
        "NARRATION_UNPARSEABLE": "No Client Info in Bank Narration",
        "NON_CLIENT_NARRATION":  "Payment from Non-Client",
        "CPV_CPD_UNDETERMINED":  "Invoice Type Could Not Be Determined",
        "NO_NARRATION":          "No Narration Available",
    }
    return _LEGACY.get(str(val).strip(), str(val) if val else "")


# ── Aging bucket — strip letter prefix ───────────────────────────────────────
# "A: 0 - 30" → "0–30 Days"  |  "I: > 360" → "> 360 Days"

_BUCKET_LABEL_MAP = {
    "A: 0 - 30":    "0–30 Days",
    "B: 31 - 45":   "31–45 Days",
    "C: 46 - 60":   "46–60 Days",
    "D: 61 - 90":   "61–90 Days",
    "E: 91 - 120":  "91–120 Days",
    "F: 121 - 150": "121–150 Days",
    "G: 151 - 180": "151–180 Days",
    "H: 181 - 360": "181–360 Days",
    "I: > 360":     "> 360 Days",
}

def translate_bucket(val: str) -> str:
    return _BUCKET_LABEL_MAP.get(str(val).strip(), val)


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 2 — COLUMN VISIBILITY LISTS
# ══════════════════════════════════════════════════════════════════════════════
#
# Each sheet has two lists:
#   STREAMLIT_COLS  — display columns in the app (order matters)
#   EXCEL_COLS      — full column set in the Excel download (order matters)
#
# Column names here are the DISPLAY names (after rename), not internal names.
# The apply_* functions below handle the rename + reorder.

# ── Reconciliation Statement ──────────────────────────────────────────────────

STATEMENT_COL_RENAME = {
    "Bill_Ref":          "Bill Ref",
    "Bill_Date":         "Bill Date",
    "Bill_Amount":       f"Bill Amt ({INR})",
    "Cleared_Date":      "Payment Date",
    "Cleared_Amount":    f"Amount Settled ({INR})",
    "Remaining_After":   f"Balance After ({INR})",
    "Match_Confidence":  "Confidence",
    "Match_Method":      "Method",
}

STATEMENT_STREAMLIT_COLS = [
    "Vertical", "Client", "Bill Ref", "Bill Date",
    f"Bill Amt ({INR})", "Payment Date", f"Amount Settled ({INR})",
    f"Balance After ({INR})", "Confidence",
]

STATEMENT_EXCEL_COLS = [
    "Vertical", "Client", "Bill Ref", "Bill Date",
    f"Bill Amt ({INR})", "Cleared By", "Cleared Ref", "Payment Date",
    f"Amount Settled ({INR})", f"Balance After ({INR})",
    "Confidence", "Method", "Narration",
]


# ── Updated Debtors ───────────────────────────────────────────────────────────

DEBTORS_COL_RENAME = {
    "Date":              "Bill Date",
    "Ref. No.":          "Invoice Ref",
    "Party's Name":      "Client",
    "Pending Amount":    f"Original Amount ({INR})",
    "Cleared_Amount":    f"Settled ({INR})",
    "Remaining_Amount":  f"Outstanding ({INR})",
    "Days Bucket":       "Aging Bucket",
}

DEBTORS_STREAMLIT_COLS = [
    "Vertical", "Client", "Aging Bucket", "Bill Date", "Invoice Ref",
    f"Original Amount ({INR})", f"Settled ({INR})", f"Outstanding ({INR})", "Status",
]

DEBTORS_EXCEL_COLS = [
    "Vertical", "Client", "Aging Bucket", "Bill Date", "Invoice Ref",
    f"Original Amount ({INR})", f"Settled ({INR})", f"Outstanding ({INR})",
    "Status", "Cleared By", "Cleared Ref", "State",
]


# ── KB Gaps ───────────────────────────────────────────────────────────────────

KB_GAPS_COL_RENAME = {
    "Raw_Text":          "Remitter Identified As",
    "Amount":            f"Payment Amount ({INR})",
    "Suggested_Client":  "Matched To",
    "Action_Required":   "Gap Reason",
    "Source":            "Payment Source",
}

KB_GAPS_STREAMLIT_COLS = [
    "Vertical", "Bill Date", "Remitter Identified As",
    f"Payment Amount ({INR})", "Matched To",
    f"Open Bills ({INR})", "Gap Reason", "Payment Source",
    "Confirmed_Client",   # editable column — keep visible
]

KB_GAPS_EXCEL_COLS = [
    "Vertical", "Bill Date", "Remitter Identified As",
    f"Payment Amount ({INR})", "Matched To",
    f"Open Bills ({INR})", "Gap Reason", "Payment Source",
    "Confirmed_Client", "Narration", "Method",
]


# ── Unresolvable ──────────────────────────────────────────────────────────────

UNRESOLVABLE_COL_RENAME = {
    "Raw_Text":  "Received Via",
    "Reason":    "Why Unresolved",
    "Amount":    f"Amount ({INR})",
}

UNRESOLVABLE_STREAMLIT_COLS = [
    "Vertical", "Bill Date", f"Amount ({INR})", "Received Via", "Why Unresolved",
]

UNRESOLVABLE_EXCEL_COLS = [
    "Vertical", "Bill Date", f"Amount ({INR})", "Received Via",
    "Why Unresolved", "Narration", "Payment Source",
]


# ── Updated Suspense ──────────────────────────────────────────────────────────

# Credit column is dynamic — substituted at apply time
SUSPENSE_COL_RENAME_TEMPLATE = {
    "Particulars":       "Received Into",
    "Applied_Amount":    f"Applied to Invoice ({INR})",
    "Remaining_Amount":  f"Carry Forward ({INR})",
}

SUSPENSE_STREAMLIT_COLS = [
    "Vertical", "Bill Date", "Received Into",
    f"Total Received ({INR})",
    f"Applied to Invoice ({INR})", f"Carry Forward ({INR})", "Status",
]

SUSPENSE_EXCEL_COLS = [
    "Vertical", "Bill Date", "Received Into",
    f"Total Received ({INR})",
    f"Applied to Invoice ({INR})", f"Carry Forward ({INR})",
    "Status", "Narration",
]


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 3 — COMPUTED COLUMNS
# ══════════════════════════════════════════════════════════════════════════════

def add_open_bills_column(kb_gaps_df: pd.DataFrame,
                          updated_debtors_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'Open Bills (₹)' column to KB Gaps DataFrame.

    For each KB Gaps row, looks up all open bill Remaining_Amount values
    for the Suggested_Client in updated_debtors, formats them as a
    pipe-separated string e.g. "₹45,000 | ₹80,000 | ₹1,20,000".

    Only includes bills where Outstanding > 0 (i.e. not fully cleared).

    Args:
        kb_gaps_df:         KB Gaps DataFrame (internal column names)
        updated_debtors_df: Updated Debtors DataFrame (internal column names)

    Returns:
        kb_gaps_df with new 'Open Bills (₹)' column inserted after 'Suggested_Client'
    """
    if kb_gaps_df.empty:
        kb_gaps_df[f"Open Bills ({INR})"] = ""
        return kb_gaps_df

    df = kb_gaps_df.copy()

    if updated_debtors_df.empty or "Party's Name" not in updated_debtors_df.columns:
        df[f"Open Bills ({INR})"] = ""
        return df

    # Build lookup: base client name → sorted list of open Remaining_Amount values (FIFO)
    debtors = updated_debtors_df.copy()
    rem_col = "Remaining_Amount" if "Remaining_Amount" in debtors.columns else None
    date_col = "Date" if "Date" in debtors.columns else None

    if not rem_col:
        df[f"Open Bills ({INR})"] = ""
        return df

    debtors[rem_col] = pd.to_numeric(debtors[rem_col], errors="coerce").fillna(0)
    open_bills = debtors[debtors[rem_col] > 0].copy()

    if open_bills.empty:
        df[f"Open Bills ({INR})"] = ""
        return df

    # Normalise party name for lookup (strip branch suffix)
    # Uses _extract_client_base from resolver (SINGLE SOURCE — R2)
    open_bills["_base"] = open_bills["Party's Name"].apply(_extract_client_base)

    # Sort FIFO if date available
    if date_col:
        open_bills["_dp"] = pd.to_datetime(
            open_bills[date_col], format="%d-%b-%Y", errors="coerce"
        )
        open_bills = open_bills.sort_values("_dp", na_position="last")

    # Build {base_name: [amt1, amt2, ...]} dict
    bills_lookup: dict[str, list] = {}
    for _, row in open_bills.iterrows():
        key = row["_base"]
        amt = row[rem_col]
        bills_lookup.setdefault(key, []).append(amt)

    def _format_bills(suggested_client) -> str:
        if not suggested_client or pd.isna(suggested_client):
            return ""
        base = _extract_client_base(str(suggested_client))
        amts = bills_lookup.get(base, [])
        if not amts:
            return "No open bills"
        return " | ".join(f"{INR}{a:,.0f}" for a in amts)

    df[f"Open Bills ({INR})"] = df["Suggested_Client"].apply(_format_bills)
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  APPLY FUNCTIONS — called by report_builder.py and app_test.py
# ══════════════════════════════════════════════════════════════════════════════

def apply_statement_presentation(df: pd.DataFrame,
                                  for_excel: bool = False) -> pd.DataFrame:
    """
    Rename columns, translate Method + Confidence values, select visible columns.
    for_excel=True  → EXCEL_COLS (all columns including audit trail)
    for_excel=False → STREAMLIT_COLS (reviewer-facing only)
    """
    if df.empty:
        return df
    out = df.copy()

    # Translate values before rename
    if "Match_Method" in out.columns:
        out["Match_Method"] = out["Match_Method"].apply(translate_method)
    if "Match_Confidence" in out.columns:
        out["Confidence_Tier"] = out["Match_Confidence"].apply(translate_confidence)
        # For Excel: keep numeric for colour-coding; add tier as separate col
        # For Streamlit: replace with tier label
        if not for_excel:
            out["Match_Confidence"] = out["Confidence_Tier"]
    if "Cleared_By" in out.columns:
        out = out.rename(columns={"Cleared_By": "Cleared By"})
    if "Cleared_Ref" in out.columns:
        out = out.rename(columns={"Cleared_Ref": "Cleared Ref"})

    out = out.rename(columns=STATEMENT_COL_RENAME)

    # Drop internal tier col used above
    out = out.drop(columns=["Confidence_Tier"], errors="ignore")

    target_cols = STATEMENT_EXCEL_COLS if for_excel else STATEMENT_STREAMLIT_COLS
    visible = [c for c in target_cols if c in out.columns]
    return out[visible]


def apply_debtors_presentation(df: pd.DataFrame,
                                for_excel: bool = False) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    if "Status" in out.columns:
        out["Status"] = out["Status"].apply(translate_debtors_status)
    if "Days Bucket" in out.columns:
        out["Days Bucket"] = out["Days Bucket"].apply(translate_bucket)
    if "_Status" in out.columns:
        out = out.drop(columns=["_Status"], errors="ignore")
    if "Cleared_By" in out.columns:
        out = out.rename(columns={"Cleared_By": "Cleared By"})
    if "Cleared_Ref" in out.columns:
        out = out.rename(columns={"Cleared_Ref": "Cleared Ref"})

    out = out.rename(columns=DEBTORS_COL_RENAME)

    target_cols = DEBTORS_EXCEL_COLS if for_excel else DEBTORS_STREAMLIT_COLS
    visible = [c for c in target_cols if c in out.columns]
    return out[visible]


def apply_kb_gaps_presentation(df: pd.DataFrame,
                                updated_debtors_df: pd.DataFrame,
                                for_excel: bool = False) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    # Add computed column (uses internal names — must run BEFORE rename)
    out = add_open_bills_column(out, updated_debtors_df)

    if "Action_Required" in out.columns:
        out["Action_Required"] = out["Action_Required"].apply(translate_gap_reason)
    if "Method" in out.columns and for_excel:
        out["Method"] = out["Method"].apply(translate_method)

    # Rename Date → Bill Date
    if "Date" in out.columns:
        out = out.rename(columns={"Date": "Bill Date"})

    out = out.rename(columns=KB_GAPS_COL_RENAME)

    # Ensure Confirmed_Client column exists
    if "Confirmed_Client" not in out.columns:
        insert_after = "Matched To"
        if insert_after in out.columns:
            idx = out.columns.get_loc(insert_after) + 1
            out.insert(idx, "Confirmed_Client", "")
        else:
            out["Confirmed_Client"] = ""

    target_cols = KB_GAPS_EXCEL_COLS if for_excel else KB_GAPS_STREAMLIT_COLS
    visible = [c for c in target_cols if c in out.columns]
    return out[visible]


def apply_unresolvable_presentation(df: pd.DataFrame,
                                     for_excel: bool = False) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    if "Reason" in out.columns:
        out["Reason"] = out["Reason"].apply(translate_unresolvable_reason)
    if "Date" in out.columns:
        out = out.rename(columns={"Date": "Bill Date"})
    if "Source" in out.columns:
        out = out.rename(columns={"Source": "Payment Source"})

    out = out.rename(columns=UNRESOLVABLE_COL_RENAME)

    target_cols = UNRESOLVABLE_EXCEL_COLS if for_excel else UNRESOLVABLE_STREAMLIT_COLS
    visible = [c for c in target_cols if c in out.columns]
    return out[visible]


def apply_suspense_presentation(df: pd.DataFrame,
                                 for_excel: bool = False) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    if "Status" in out.columns:
        out["Status"] = out["Status"].apply(translate_suspense_status)
    if "Date" in out.columns:
        out = out.rename(columns={"Date": "Bill Date"})

    # Dynamic credit column rename
    credit_col = next((c for c in out.columns if c.lower() == "credit"), None)
    if credit_col:
        out = out.rename(columns={credit_col: f"Total Received ({INR})"})

    out = out.rename(columns=SUSPENSE_COL_RENAME_TEMPLATE)

    # Build target cols — substituting dynamic credit col name
    streamlit = [c.replace("<<CREDIT>>", f"Total Received ({INR})")
                 for c in SUSPENSE_STREAMLIT_COLS]
    excel_    = [c.replace("<<CREDIT>>", f"Total Received ({INR})")
                 for c in SUSPENSE_EXCEL_COLS]

    target_cols = excel_ if for_excel else streamlit
    visible = [c for c in target_cols if c in out.columns]
    return out[visible]


def translate_aging_buckets(pivot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename aging bucket columns in the pivot table.
    'A: 0 - 30' → '0–30 Days' etc.
    Also renames 'Grand Total' → 'Total Outstanding (₹)'.
    """
    rename = {k: v for k, v in _BUCKET_LABEL_MAP.items() if k in pivot_df.columns}
    rename["Grand Total"] = f"Total Outstanding ({INR})"
    return pivot_df.rename(columns=rename)