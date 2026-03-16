"""
backend/reconciler/engine.py — Reconciliation engine.
================================================================================
Responsibility: answer "Which bill does it clear?"

Contains:
  - _BillApplicator (private) — bill-level FIFO matching + mutation
  - Reconciler class — orchestrates Steps 1, 2, 2b for a single vertical
  - run_all_verticals — multi-vertical runner + result aggregation

Does NOT handle: client name resolution, KB matching, narration parsing.
Those belong in resolver.py.
================================================================================
"""

import re
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
from datetime import datetime

from backend.config import (
    DATA_DIR,
    MATCH_TOLERANCE,
    CLEARING_ROUNDING_THRESHOLD,
    HIGH_CONF,    # Reserved for future confidence-tiered review workflow
    LOW_CONF,
    BRANCH_FUZZY_THRESHOLD,
)
from backend.common import (
    _normalise_base, _normalise_for_fuzzy, _fuzzy,
    BillStatus, SuspenseStatus, GapReason, UnresolvableReason,
)
from backend.reconciler.resolver import (
    Validator, ClientResolver, _build_kb_bridge,
    _parse_neft_client, _is_non_client,
    _extract_client_base, _extract_branch,
)
from backend.parsers.debtors import (
    standardise_party_name,
    extract_ref_prefix,
    _normalise_key,
)


# Vertical that needs CPV/CPD sub-split
CREDIT_VERTICAL = "Credit"

_SUSPENSE_PARTICULARS = {"SUSPENSE", "SUSPENSE A/C", "SUSPENSE AC", "SUSPENSE ACCOUNT", "SUSPENSE LEDGER"}

# Minimum fraction of bill amount for a single payment to be treated as
# an underpayment in Step 2. Payments below this fraction are left pending
# so Step 2b can aggregate them into a matching group sum.
MIN_PARTIAL_RATIO = 0.5


def _within_tolerance(inv_amt: float, pay_amt: float, tol: float = MATCH_TOLERANCE) -> bool:
    """True if pay_amt is within tol% of inv_amt."""
    if inv_amt <= 0:
        return False
    return abs(inv_amt - pay_amt) / inv_amt <= tol


class Reconciler:
    """
    Vertical-wise reconciliation engine.

    Usage:
        r = Reconciler(kb_path='master_client_knowledge_base.json')
        results = r.reconcile(debtors_df, receipts_df, suspense_df, vertical)

    Returns dict with keys:
        statement         → per knock-off audit trail
        updated_debtors   → D1 with status + cleared amounts
        updated_suspense  → D3 with remaining amounts
        kb_gaps           → unmatched with candidate suggestions
        unresolvable      → completely unknown entries
        summary           → counts and totals
    """

    def __init__(self, kb_path: str = str(DATA_DIR / "master_client_knowledge_base.json"),
                 validator: Validator = None, kb_bridge: dict = None):
        """
        R9: Dependency injection — accepts pre-built validator and bridge.
        If not provided, falls back to loading from kb_path (original behavior).
        This allows unit-testing with mock validators without touching the filesystem.
        """
        self.kb_path = kb_path
        self.validator = validator
        self.kb_bridge = kb_bridge if kb_bridge is not None else {}
        # Fallback: load from file if not injected
        if self.validator is None and kb_path and Path(kb_path).exists():
            self.validator = Validator(kb_path)
            self.kb_bridge = _build_kb_bridge(kb_path)

    # ── Public entry point ────────────────────────────────────────────────────

    def reconcile(
        self,
        debtors_df:  pd.DataFrame,
        receipts_df: pd.DataFrame,
        suspense_df: pd.DataFrame,
        vertical:    str,
    ) -> dict:
        """
        Run full reconciliation for one vertical.

        Args:
            debtors_df:  parsed debtors ageing for this vertical
            receipts_df: parsed receipts for this vertical
            suspense_df: parsed suspense for this vertical
            vertical:    vertical label (e.g. 'Credit', 'Legal', 'GFGC')

        Returns:
            dict of result DataFrames + summary
        """
        print("=" * 60)
        print(f"RECONCILING: {vertical}")
        print(f"  Debtors : {len(debtors_df):,} bills")
        print(f"  Receipts: {len(receipts_df):,} entries")
        print(f"  Suspense: {len(suspense_df):,} entries")
        print("=" * 60)

        # Build working copies — we mutate Pending Amount as we go
        d1 = debtors_df.copy().reset_index(drop=True)
        d1["_Remaining"]      = pd.to_numeric(d1["Pending Amount"], errors="coerce").fillna(0).astype(float)
        d1["_Status"]         = BillStatus.OPEN
        d1["_Cleared_Amount"] = 0.0
        d1["_Cleared_By"]    = ""
        d1["_Cleared_Ref"]   = ""
        # #28: Pre-compute base name column — avoids repeated .apply(lambda) in hot loops
        # _normalise_base folds LTD / LTD. / LIMITED so that resolver output and
        # debtor entries entered with different abbreviations still match.
        d1["_base_name"]     = d1["Party's Name"].apply(
            lambda x: _normalise_base(_extract_client_base(str(x))) if pd.notna(x) else ""
        )

        # Build suspense working copy
        s3 = suspense_df.copy().reset_index(drop=True)
        # Credit column = inflow amount
        credit_col = "Credit"
        if credit_col not in s3.columns:
            credit_col = next((c for c in s3.columns if "credit" in c.lower()), None)

        if credit_col is None:
            available_cols = list(s3.columns)
            raise ValueError(
                f"Suspense file for vertical '{vertical}' has no recognisable credit/inflow column. "
                f"Available columns: {available_cols}. "
                f"Expected a column named 'Credit' or containing 'credit'. "
                f"Check that the correct Suspense file was uploaded for this vertical."
            )

        s3["_Remaining"]  = pd.to_numeric(s3[credit_col], errors="coerce").fillna(0).astype(float)
        s3["_Applied"]    = 0.0
        s3["_Status"]     = SuspenseStatus.OPEN
        s3["_cached_resolved_name"] = ""
        s3["_cached_confidence"]    = 0.0
        s3["_cached_method"]        = ""

        # Client resolver — built once per vertical, using pre-loaded validator
        all_debtors_names = d1["Party's Name"].dropna().unique().tolist()
        resolver = ClientResolver(
            self.kb_path, all_debtors_names,
            validator=self.validator, kb_bridge=self.kb_bridge,
        )

        # Accumulators
        statement_rows  = []
        kb_gaps_rows    = []
        unresolvable_rows = []

        # ── Step 1: Direct receipts → Debtors ────────────────────────────────
        print("\n  Step 1: Direct receipts → Debtors...")
        d1, s3, step1_statement, step1_gaps, step1_unresolvable = self._apply_direct_receipts(
            d1, s3, receipts_df, resolver, vertical
        )
        statement_rows    += step1_statement
        kb_gaps_rows      += step1_gaps
        unresolvable_rows += step1_unresolvable
        print(f"    → {len(step1_statement)} matches, {len(step1_gaps)} KB gaps, {len(step1_unresolvable)} unresolvable")

        # ── Step 2: Suspense → remaining Debtors ─────────────────────────────
        print("\n  Step 2: Suspense credits → Debtors...")
        d1, s3, step2_statement, step2_gaps, step2_unresolvable = self._apply_suspense(
            d1, s3, resolver, vertical
        )
        statement_rows    += step2_statement
        kb_gaps_rows      += step2_gaps
        unresolvable_rows += step2_unresolvable
        print(f"    → {len(step2_statement)} matches, {len(step2_gaps)} KB gaps, {len(step2_unresolvable)} unresolvable")

        # ── Step 2b: Aggregation pass — multi-payment → single bill ─────────────
        # Runs after 1-to-1 pass. Groups unmatched suspense entries by resolved
        # client + calendar month and tries to match the cumulative sum against
        # remaining open bills. Handles instalment payers (Motilal Oswal, UGRO etc.)
        print("\n  Step 2b: Aggregation pass (multi-payment → single bill)...")
        d1, s3, step2b_statement, step2b_gaps = self._apply_suspense_aggregated(
            d1, s3, resolver, vertical
        )
        statement_rows += step2b_statement
        kb_gaps_rows   += step2b_gaps
        print(f"    → {len(step2b_statement)} aggregate matches, {len(step2b_gaps)} remaining gaps")

        # Remove stale Amount Mismatch kb_gaps that Step 2b has since resolved.
        # A suspense entry is considered resolved if its _Remaining dropped to 0
        # after the aggregation pass. Match on narration as the join key.
        resolved_narrations = set(
            s3.loc[s3["_Remaining"] <= CLEARING_ROUNDING_THRESHOLD, "Narration"]
            .astype(str)
            .str.strip()
        )
        kb_gaps_rows = [
            gap for gap in kb_gaps_rows
            if not (
                gap.get("Action_Required") == GapReason.AMOUNT_MISMATCH
                and str(gap.get("Narration", "")).strip() in resolved_narrations
            )
        ]

        # ── Step 2c: Cross-branch aggregation pass ────────────────────────────
        # Groups by (resolved_BASE, quarter) — strips branch suffix so payments
        # tagged to different branches (e.g. Cholamandalam_Jalna + _Ahmednagar)
        # pool together and match against any open bill for that client.
        print("\n  Step 2c: Cross-branch aggregation pass (multi-branch → single bill)...")
        d1, s3, stmt_2c, gaps_2c = self._apply_suspense_cross_branch(
            d1, s3, resolver, vertical
        )
        statement_rows.extend(stmt_2c)
        kb_gaps_rows.extend(gaps_2c)
        print(f"    → {len(stmt_2c)} cross-branch matches, {len(gaps_2c)} remaining gaps")

        # Remove stale Amount Mismatch gaps resolved by Step 2c.
        resolved_narrations_2c = set(
            s3.loc[s3["_Remaining"] <= CLEARING_ROUNDING_THRESHOLD, "Narration"]
            .astype(str)
            .str.strip()
        )
        kb_gaps_rows = [
            gap for gap in kb_gaps_rows
            if not (
                gap.get("Action_Required") == GapReason.AMOUNT_MISMATCH
                and str(gap.get("Narration", "")).strip() in resolved_narrations_2c
            )
        ]

        # ── Step 3: Build outputs ─────────────────────────────────────────────
        updated_debtors  = self._build_updated_debtors(d1)
        updated_suspense = self._build_updated_suspense(s3, credit_col)
        statement_df     = pd.DataFrame(statement_rows) if statement_rows else pd.DataFrame()
        unresolvable_df  = pd.DataFrame(unresolvable_rows) if unresolvable_rows else pd.DataFrame()

        # Split KB gaps — advance payments get their own sheet so they don't
        # pollute the genuine gaps that require KB additions / investigation.
        # "No open bills" = client resolved correctly but all bills already cleared.
        advance_rows  = [r for r in kb_gaps_rows
                         if str(r.get("Action_Required", "")).strip()
                         == GapReason.NO_OPEN_BILLS]
        clean_gap_rows = [r for r in kb_gaps_rows
                          if str(r.get("Action_Required", "")).strip()
                          != GapReason.NO_OPEN_BILLS]

        kb_gaps_df         = pd.DataFrame(clean_gap_rows)  if clean_gap_rows  else pd.DataFrame()
        advance_payments_df = pd.DataFrame(advance_rows)   if advance_rows    else pd.DataFrame()

        # Summary
        total_debtors_initial = d1["Pending Amount"].apply(
            lambda x: pd.to_numeric(x, errors="coerce")
        ).sum()
        total_cleared = d1["_Cleared_Amount"].sum()
        total_outstanding = d1["_Remaining"].sum()
        total_suspense_initial = s3["_Remaining"].sum() + s3["_Applied"].sum()
        total_suspense_applied = s3["_Applied"].sum()
        total_suspense_remaining = s3["_Remaining"].sum()

        summary = {
            "vertical":                vertical,
            "debtors_bills":           len(d1),
            "bills_cleared":           int((d1["_Status"] == BillStatus.CLEARED).sum()),
            "bills_partial":           int((d1["_Status"] == BillStatus.PARTIALLY_CLEARED).sum()),
            "bills_open":              int((d1["_Status"] == BillStatus.OPEN).sum()),
            "total_debtors_initial":   round(total_debtors_initial, 2),
            "total_cleared":           round(total_cleared, 2),
            "total_outstanding":       round(total_outstanding, 2),
            "suspense_entries":        len(s3),
            "suspense_applied":        round(total_suspense_applied, 2),
            "suspense_remaining":      round(total_suspense_remaining, 2),
            "statement_entries":       len(statement_rows),
            "kb_gaps":                 len(clean_gap_rows),
            "advance_payments":        len(advance_rows),
            "unresolvable":            len(unresolvable_rows),
        }

        print(f"\n  Results for {vertical}:")
        print(f"    Bills cleared     : {summary['bills_cleared']:,}")
        print(f"    Bills partial     : {summary['bills_partial']:,}")
        print(f"    Bills open        : {summary['bills_open']:,}")
        print(f"    Total cleared     : ₹{summary['total_cleared']:,.2f}")
        print(f"    Outstanding       : ₹{summary['total_outstanding']:,.2f}")
        print(f"    Suspense applied  : ₹{summary['suspense_applied']:,.2f}")
        print(f"    Suspense remaining: ₹{summary['suspense_remaining']:,.2f}")
        print(f"    KB gaps           : {summary['kb_gaps']:,}")
        print(f"    Advance payments  : {summary['advance_payments']:,}")
        print(f"    Unresolvable      : {summary['unresolvable']:,}")

        return {
            "statement":          statement_df,
            "updated_debtors":    updated_debtors,
            "updated_suspense":   updated_suspense,
            "kb_gaps":            kb_gaps_df,
            "advance_payments":   advance_payments_df,
            "unresolvable":       unresolvable_df,
            "summary":            summary,
        }

    # ── Step 1: Direct receipts ───────────────────────────────────────────────

    def _apply_direct_receipts(
        self, d1, s3, receipts_df, resolver, vertical
    ):
        statement  = []
        kb_gaps    = []
        unresolvable = []

        # Filter: only direct receipts (Particulars != 'Suspense')
        particulars_col = "Particulars"
        direct = receipts_df[
            ~receipts_df[particulars_col].str.strip().str.upper().isin(_SUSPENSE_PARTICULARS)
        ].copy()

        if direct.empty:
            return d1, s3, statement, kb_gaps, unresolvable

        # Sort FIFO
        direct["_date_parsed"] = pd.to_datetime(direct["Date"], format="%d-%b-%Y", errors="coerce")
        direct = direct.sort_values("_date_parsed", na_position="last").reset_index(drop=True)

        for _, rec in direct.iterrows():
            raw_text   = str(rec.get(particulars_col, "")).strip()
            debit_amt  = pd.to_numeric(rec.get("Debit"), errors="coerce")

            if pd.isna(debit_amt) or debit_amt <= 0:
                continue

            # CPV/CPD prefix detection for Credit vertical
            prefix = None
            if vertical == CREDIT_VERTICAL:
                vch_no = str(rec.get("Vch No.", rec.get("Vch No", ""))).strip()
                prefix = extract_ref_prefix(vch_no)
                if not prefix:
                    # Cannot determine CPV/CPD — unresolvable
                    unresolvable.append({
                        "Vertical":     vertical,
                        "Source":       "Receipt",
                        "Date":         rec.get("Date"),
                        "Raw_Text":     raw_text,
                        "Amount":       debit_amt,
                        "Reason":       UnresolvableReason.CPV_CPD_UNDETERMINED,
                        "Vch_No":       vch_no,
                        "Narration":    rec.get("Narration", ""),
                    })
                    continue

            # Resolve client name
            resolution = resolver.resolve(raw_text)

            if resolution.resolved_name is None:
                # KB gap or unresolvable
                entry = {
                    "Vertical":          vertical,
                    "Source":            "Receipt",
                    "Date":              rec.get("Date"),
                    "Raw_Text":          raw_text,
                    "Amount":            debit_amt,
                    "Narration":         rec.get("Narration", ""),
                    "Method":            resolution.method,
                    "Suggested_Client":  resolution.suggestion or "",
                    "Action_Required":   GapReason.UNKNOWN_CLIENT,
                }
                kb_gaps.append(entry)
                continue

            if resolution.confidence < LOW_CONF:
                unresolvable.append({
                    "Vertical":  vertical,
                    "Source":    "Receipt",
                    "Date":      rec.get("Date"),
                    "Raw_Text":  raw_text,
                    "Amount":    debit_amt,
                    "Reason":    UnresolvableReason.CONFIDENCE_TOO_LOW,
                    "Narration": rec.get("Narration", ""),
                })
                continue

            resolved_name = resolution.resolved_name

            # Find matching debtors bills
            matched = self._find_and_apply(
                d1           = d1,
                resolved_name= resolved_name,
                pay_amount   = debit_amt,
                vertical     = vertical,
                prefix       = prefix,
                source_type  = "Receipt",
                source_ref   = str(rec.get("Vch No.", rec.get("Vch No", ""))),
                source_date  = rec.get("Date"),
                raw_text     = raw_text,
                narration    = rec.get("Narration", ""),
                confidence   = resolution.confidence,
                method       = resolution.method,
                statement    = statement,
                kb_gaps      = kb_gaps,
            )

        return d1, s3, statement, kb_gaps, unresolvable

    # ── Step 2: Suspense → Debtors ────────────────────────────────────────────

    def _apply_suspense(self, d1, s3, resolver, vertical):
        statement    = []
        kb_gaps      = []
        unresolvable = []

        # Sort suspense FIFO
        s3["_date_parsed"] = pd.to_datetime(s3["Date"], format="%d-%b-%Y", errors="coerce")
        s3_sorted = s3.sort_values("_date_parsed", na_position="last")

        for idx, susp in s3_sorted.iterrows():
            remaining = s3.at[idx, "_Remaining"]
            if remaining <= 0:
                continue

            narration = str(susp.get("Narration", "")).strip()
            if not narration:
                # No narration — QR or unidentifiable
                unresolvable.append({
                    "Vertical":   vertical,
                    "Source":     "Suspense",
                    "Date":       susp.get("Date"),
                    "Raw_Text":   str(susp.get("Particulars", "")),
                    "Amount":     remaining,
                    "Reason":     UnresolvableReason.NO_NARRATION,
                    "Narration":  "",
                })
                continue

            # Extract client hint from NEFT string
            client_hint = _parse_neft_client(narration)
            if not client_hint:
                unresolvable.append({
                    "Vertical":   vertical,
                    "Source":     "Suspense",
                    "Date":       susp.get("Date"),
                    "Raw_Text":   str(susp.get("Particulars", "")),
                    "Amount":     remaining,
                    "Reason":     UnresolvableReason.NARRATION_UNPARSEABLE,
                    "Narration":  narration,
                })
                continue

            # Gate: reject bank internals, individual UPI, routing artifacts
            # before they reach KB matching and pollute KB Gaps output.
            if _is_non_client(client_hint):
                unresolvable.append({
                    "Vertical":   vertical,
                    "Source":     "Suspense",
                    "Date":       susp.get("Date"),
                    "Raw_Text":   client_hint,
                    "Amount":     remaining,
                    "Reason":     UnresolvableReason.NON_CLIENT_NARRATION,
                    "Narration":  narration,
                })
                continue

            # Resolve client
            resolution = resolver.resolve(client_hint)

            if resolution.resolved_name is None:
                entry = {
                    "Vertical":          vertical,
                    "Source":            "Suspense",
                    "Date":              susp.get("Date"),
                    "Raw_Text":          client_hint,
                    "Amount":            remaining,
                    "Narration":         narration,
                    "Method":            resolution.method,
                    "Suggested_Client":  resolution.suggestion or "",
                    "Action_Required":   GapReason.UNKNOWN_CLIENT,
                }
                kb_gaps.append(entry)
                continue

            if resolution.confidence < LOW_CONF:
                unresolvable.append({
                    "Vertical":  vertical,
                    "Source":    "Suspense",
                    "Date":      susp.get("Date"),
                    "Raw_Text":  client_hint,
                    "Amount":    remaining,
                    "Reason":    UnresolvableReason.CONFIDENCE_TOO_LOW,
                    "Narration": narration,
                })
                continue

            resolved_name = resolution.resolved_name
            s3.at[idx, "_cached_resolved_name"] = resolved_name
            s3.at[idx, "_cached_confidence"]    = resolution.confidence
            s3.at[idx, "_cached_method"]        = resolution.method

            # CPV/CPD for suspense in Credit vertical
            # Try to infer from narration or ref if available
            prefix = None
            if vertical == CREDIT_VERTICAL:
                vch_no = str(susp.get("Vch No.", susp.get("Vch No", ""))).strip()
                prefix = extract_ref_prefix(vch_no)
                # For suspense, CPV/CPD undetermined is NOT an error —
                # we try to match against whichever prefix bill amount fits
                # If still unresolvable after amount matching, it goes to KB gaps

            matched = self._find_and_apply(
                d1            = d1,
                resolved_name = resolved_name,
                pay_amount    = remaining,
                vertical      = vertical,
                prefix        = prefix,
                source_type   = "Suspense",
                source_ref    = str(susp.get("Vch No.", susp.get("Vch No", ""))),
                source_date   = susp.get("Date"),
                raw_text      = client_hint,
                narration     = narration,
                confidence    = resolution.confidence,
                method        = resolution.method,
                statement     = statement,
                kb_gaps       = kb_gaps,
                suspense_idx  = idx,
                s3            = s3,
            )

        return d1, s3, statement, kb_gaps, unresolvable

    # ── Step 2b: Aggregation pass ─────────────────────────────────────────────

    def _apply_suspense_aggregated(self, d1, s3, resolver, vertical):
        """
        Second-pass suspense matching: aggregate multiple payments from the same
        client within a calendar month and match the cumulative sum against a
        single open bill.

        Targets clients who pay in monthly instalments against a consolidated bill
        (e.g. Motilal Oswal 50+ small entries → one quarterly bill, UGRO 4x ₹1,080
        → one ₹4,320 bill). The 1-to-1 pass leaves these unmatched because no
        single payment hits within the 3% tolerance of the bill amount.

        Algorithm:
            1. Collect suspense entries still open after Step 2 (_Remaining > 0)
               that have a resolvable client (narration parseable + KB/fuzzy match).
            2. Group by (resolved_client_base, calendar_month).
            3. For each group, attempt to match the group sum against the oldest
               open bill for that client within tolerance.
            4. If match: apply all entries in the group proportionally, FIFO across
               multiple bills if group sum exceeds one bill.
            5. If no match: leave entries open (they stay in KB gaps from Step 2).

        Does NOT re-process entries that were already fully applied in Step 2.
        Does NOT generate new KB gaps — unmatched aggregates were already logged.
        """
        statement = []
        kb_gaps   = []

        # ── Collect still-open suspense entries with parseable narrations ──────
        open_mask = s3["_Remaining"] > 0
        if not open_mask.any():
            return d1, s3, statement, kb_gaps

        # Re-parse and resolve every open entry to get client + month
        # We need (suspense_idx, resolved_base, month, remaining, narration, confidence, method)
        entry_meta = []
        for idx, susp in s3[open_mask].iterrows():
            narration = str(susp.get("Narration", "")).strip()
            if not narration:
                continue
            cached_name = s3.at[idx, "_cached_resolved_name"] if "_cached_resolved_name" in s3.columns else ""
            if not cached_name:
                # Step 2 resolution failed for this entry — attempt independent resolution.
                # This is the exact case the aggregation pass was built for: clients whose
                # individual payments fail 1-to-1 matching but whose grouped sum matches a bill.
                # Do NOT skip — fall through to independent resolution.
                client_hint = _parse_neft_client(narration)
                if not client_hint:
                    continue
                resolution = resolver.resolve(client_hint)
                if resolution.resolved_name is None:
                    continue
                resolved_name = resolution.resolved_name
                cached_confidence = resolution.confidence
                cached_method = resolution.method
            else:
                resolved_name = cached_name
                cached_confidence = s3.at[idx, "_cached_confidence"] if "_cached_confidence" in s3.columns else 0.0
                cached_method     = s3.at[idx, "_cached_method"]     if "_cached_method"     in s3.columns else "CACHED"

            resolved_base = _normalise_base(_extract_client_base(resolved_name))

            # Quarter key — group payments within the same rolling quarter
            # (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec).
            # Wider than calendar month — catches Motilal Oswal, Tata Capital
            # instalment patterns where tranches trickle in over 2-3 months.
            date_str = str(susp.get("Date", ""))
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%Y")
                _q = (parsed_date.month - 1) // 3 + 1
                month_key = f"Q{_q}-{parsed_date.year}"
            except Exception:
                month_key = "UNKNOWN"

            entry_meta.append({
                "idx":            idx,
                "resolved_name":  resolved_name,
                "resolved_base":  resolved_base,
                "month_key":      month_key,
                "remaining":      s3.at[idx, "_Remaining"],
                "narration":      narration,
                "date":           susp.get("Date"),
                "vch_no":         str(susp.get("Vch No.", susp.get("Vch No", ""))),
                "confidence":     cached_confidence,
                "method":         cached_method,
            })

        if not entry_meta:
            return d1, s3, statement, kb_gaps

        meta_df = pd.DataFrame(entry_meta)

        # ── Group by client name + month ──────────────────────────────────────
        for (resolved_name_group, month_key), group in meta_df.groupby(
            ["resolved_name", "month_key"], sort=False
        ):
            resolved_base = _normalise_base(_extract_client_base(resolved_name_group))
            if len(group) < 2:
                # Single entry — already tried 1-to-1 in Step 2, skip
                continue

            group_sum = group["remaining"].sum()
            if group_sum <= 0:
                continue

            # Candidate bills for this client (any branch), still open
            candidates = d1[
                (d1["_Remaining"] > 0) &
                (d1["_base_name"] == resolved_base)
            ].copy()

            if candidates.empty:
                continue

            # CPV/CPD filter for Credit vertical
            prefix = None
            if vertical == CREDIT_VERTICAL:
                # Use first entry's vch_no as proxy
                vch_no = group.iloc[0]["vch_no"]
                prefix = extract_ref_prefix(vch_no)
                if prefix:
                    prefixed = candidates[
                        candidates["Ref. No."].apply(
                            lambda r: extract_ref_prefix(str(r)) == prefix
                        )
                    ]
                    if not prefixed.empty:
                        candidates = prefixed

            # Sort FIFO
            candidates = candidates.copy()  # avoid SettingWithCopyWarning after prefix filter
            candidates["_dp"] = pd.to_datetime(
                candidates["Date"], format="%d-%b-%Y", errors="coerce"
            )
            candidates = candidates.sort_values("_dp", na_position="last")

            # Sort group entries FIFO too
            group = group.copy()
            group["_dp"] = pd.to_datetime(
                group["date"], format="%d-%b-%Y", errors="coerce"
            )
            group = group.sort_values("_dp", na_position="last")

            # Attempt to match group_sum against bills FIFO
            remaining_pool = group_sum
            group_indices  = list(group["idx"])

            # Representative metadata from first entry
            rep = group.iloc[0]

            applied_any = False
            for bill_idx, bill in candidates.iterrows():
                if remaining_pool <= 0:
                    break

                bill_remaining = d1.at[bill_idx, "_Remaining"]
                if bill_remaining <= 0:
                    continue

                # Check if group_sum (or what's left of it) matches this bill
                if not _within_tolerance(bill_remaining, remaining_pool):
                    # Also check: if remaining_pool > bill, can we clear this bill
                    # and carry forward? Only if remaining_pool significantly > bill.
                    if remaining_pool <= bill_remaining * (1 + MATCH_TOLERANCE):
                        continue  # Not enough to clear this bill either
                    # remaining_pool > bill — apply full bill, carry remainder
                    apply_to_bill = bill_remaining
                else:
                    apply_to_bill = remaining_pool

                # Distribute apply_to_bill across group entries proportionally (FIFO)
                to_distribute = apply_to_bill
                cleared_refs  = []
                cleared_dates = []

                for entry_idx in group_indices:
                    if to_distribute <= 0:
                        break
                    entry_rem = s3.at[entry_idx, "_Remaining"]
                    if entry_rem <= 0:
                        continue
                    applied = min(entry_rem, to_distribute)
                    s3.at[entry_idx, "_Applied"]   += applied
                    s3.at[entry_idx, "_Remaining"]  = max(0, entry_rem - applied)
                    if s3.at[entry_idx, "_Remaining"] <= CLEARING_ROUNDING_THRESHOLD:
                        s3.at[entry_idx, "_Status"] = SuspenseStatus.FULLY_APPLIED
                    else:
                        s3.at[entry_idx, "_Status"] = SuspenseStatus.PARTIALLY_APPLIED
                    to_distribute -= applied
                    vch = str(s3.at[entry_idx, "Vch No."] if "Vch No." in s3.columns
                              else s3.at[entry_idx, "Vch No"] if "Vch No" in s3.columns
                              else entry_idx)
                    cleared_refs.append(vch)
                    cleared_dates.append(str(s3.at[entry_idx, "Date"]))

                # Update debtors bill
                d1.at[bill_idx, "_Remaining"]      = max(0, bill_remaining - apply_to_bill)
                d1.at[bill_idx, "_Cleared_Amount"] += apply_to_bill
                d1.at[bill_idx, "_Cleared_By"]     += (
                    (", " if d1.at[bill_idx, "_Cleared_By"] else "") + "Suspense"
                )
                d1.at[bill_idx, "_Cleared_Ref"] += (
                    (", " if d1.at[bill_idx, "_Cleared_Ref"] else "") +
                    "+".join(cleared_refs)
                )
                if d1.at[bill_idx, "_Remaining"] <= CLEARING_ROUNDING_THRESHOLD:
                    d1.at[bill_idx, "_Status"] = BillStatus.CLEARED
                else:
                    d1.at[bill_idx, "_Status"] = BillStatus.PARTIALLY_CLEARED

                statement.append({
                    "Vertical":         vertical,
                    "Client":           rep["resolved_name"],
                    "Bill_Ref":         bill.get("Ref. No.", ""),
                    "Bill_Date":        bill.get("Date", ""),
                    "Bill_Amount":      bill.get("Pending Amount", ""),
                    "Cleared_By":       "Suspense",
                    "Cleared_Ref":      "+".join(cleared_refs),
                    "Cleared_Date":     rep["date"],
                    "Cleared_Amount":   round(apply_to_bill, 2),
                    "Remaining_After":  round(d1.at[bill_idx, "_Remaining"], 2),
                    "Match_Confidence": round(rep["confidence"], 4),
                    "Match_Method":     rep["method"] + "_AGGREGATED",
                    "Narration":        rep["narration"],
                })

                remaining_pool -= apply_to_bill
                applied_any     = True

            # If the group sum didn't match anything, leave entries open.
            # They were already logged as KB gaps in Step 2 — don't double-log.

        return d1, s3, statement, kb_gaps

    # ── Step 2c: Cross-branch aggregation pass ────────────────────────────────

    def _apply_suspense_cross_branch(self, d1, s3, resolver, vertical):
        """
        Third-pass suspense matching: aggregate payments by (resolved_BASE, quarter)
        so entries tagged to different branches of the same client pool together.

        Targets clients whose payments resolve to branch-specific names
        (e.g. CHOLAMANDALAM_JALNA, CHOLAMANDALAM_AHMEDNAGAR) but whose open bills
        exist under a consolidated or different branch. Step 2b leaves these
        unmatched because it groups by resolved_name (branch-specific). This pass
        groups by resolved_base instead, ignoring the branch suffix.

        Does NOT re-process entries already fully applied.
        Does NOT generate new KB gaps — unmatched entries were already logged.
        """
        statement = []
        kb_gaps   = []

        open_mask = s3["_Remaining"] > 0
        if not open_mask.any():
            return d1, s3, statement, kb_gaps

        entry_meta = []
        for idx, susp in s3[open_mask].iterrows():
            narration = str(susp.get("Narration", "")).strip()
            if not narration:
                continue
            cached_name = s3.at[idx, "_cached_resolved_name"] if "_cached_resolved_name" in s3.columns else ""
            if not cached_name:
                client_hint = _parse_neft_client(narration)
                if not client_hint:
                    continue
                resolution = resolver.resolve(client_hint)
                if resolution.resolved_name is None:
                    continue
                resolved_name     = resolution.resolved_name
                cached_confidence = resolution.confidence
                cached_method     = resolution.method
            else:
                resolved_name     = cached_name
                cached_confidence = s3.at[idx, "_cached_confidence"] if "_cached_confidence" in s3.columns else 0.0
                cached_method     = s3.at[idx, "_cached_method"]     if "_cached_method"     in s3.columns else "CACHED"

            resolved_base = _normalise_base(_extract_client_base(resolved_name))

            date_str = str(susp.get("Date", ""))
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%Y")
                _q = (parsed_date.month - 1) // 3 + 1
                month_key = f"Q{_q}-{parsed_date.year}"
            except Exception:
                month_key = "UNKNOWN"

            entry_meta.append({
                "idx":            idx,
                "resolved_name":  resolved_name,
                "resolved_base":  resolved_base,
                "month_key":      month_key,
                "remaining":      s3.at[idx, "_Remaining"],
                "narration":      narration,
                "date":           susp.get("Date"),
                "vch_no":         str(susp.get("Vch No.", susp.get("Vch No", ""))),
                "confidence":     cached_confidence,
                "method":         cached_method,
            })

        if not entry_meta:
            return d1, s3, statement, kb_gaps

        meta_df = pd.DataFrame(entry_meta)

        # ── Group by BASE name + quarter (key difference from Step 2b) ────────
        for (resolved_base_group, month_key), group in meta_df.groupby(
            ["resolved_base", "month_key"], sort=False
        ):
            resolved_base = resolved_base_group
            if len(group) < 2:
                # Single entry — Step 2b already attempted it, skip.
                continue

            group_sum = group["remaining"].sum()
            if group_sum <= 0:
                continue

            # Candidate bills — ALL branches of this base name
            candidates = d1[
                (d1["_Remaining"] > 0) &
                (d1["_base_name"] == resolved_base)
            ].copy()

            if candidates.empty:
                continue

            # CPV/CPD filter for Credit vertical
            prefix = None
            if vertical == CREDIT_VERTICAL:
                vch_no = group.iloc[0]["vch_no"]
                prefix = extract_ref_prefix(vch_no)
                if prefix:
                    prefixed = candidates[
                        candidates["Ref. No."].apply(
                            lambda r: extract_ref_prefix(str(r)) == prefix
                        )
                    ]
                    if not prefixed.empty:
                        candidates = prefixed

            # Sort FIFO
            candidates = candidates.copy()
            candidates["_dp"] = pd.to_datetime(
                candidates["Date"], format="%d-%b-%Y", errors="coerce"
            )
            candidates = candidates.sort_values("_dp", na_position="last")

            group = group.copy()
            group["_dp"] = pd.to_datetime(
                group["date"], format="%d-%b-%Y", errors="coerce"
            )
            group = group.sort_values("_dp", na_position="last")

            remaining_pool = group_sum
            group_indices  = list(group["idx"])
            rep            = group.iloc[0]

            for bill_idx, bill in candidates.iterrows():
                if remaining_pool <= 0:
                    break

                bill_remaining = d1.at[bill_idx, "_Remaining"]
                if bill_remaining <= 0:
                    continue

                if not _within_tolerance(bill_remaining, remaining_pool):
                    if remaining_pool <= bill_remaining * (1 + MATCH_TOLERANCE):
                        continue
                    apply_to_bill = bill_remaining
                else:
                    apply_to_bill = remaining_pool

                to_distribute = apply_to_bill
                cleared_refs  = []
                cleared_dates = []

                for entry_idx in group_indices:
                    if to_distribute <= 0:
                        break
                    entry_rem = s3.at[entry_idx, "_Remaining"]
                    if entry_rem <= 0:
                        continue
                    applied = min(entry_rem, to_distribute)
                    s3.at[entry_idx, "_Applied"]   += applied
                    s3.at[entry_idx, "_Remaining"]  = max(0, entry_rem - applied)
                    if s3.at[entry_idx, "_Remaining"] <= CLEARING_ROUNDING_THRESHOLD:
                        s3.at[entry_idx, "_Status"] = SuspenseStatus.FULLY_APPLIED
                    else:
                        s3.at[entry_idx, "_Status"] = SuspenseStatus.PARTIALLY_APPLIED
                    to_distribute -= applied
                    vch = str(s3.at[entry_idx, "Vch No."] if "Vch No." in s3.columns
                              else s3.at[entry_idx, "Vch No"] if "Vch No" in s3.columns
                              else entry_idx)
                    cleared_refs.append(vch)
                    cleared_dates.append(str(s3.at[entry_idx, "Date"]))

                d1.at[bill_idx, "_Remaining"]      = max(0, bill_remaining - apply_to_bill)
                d1.at[bill_idx, "_Cleared_Amount"] += apply_to_bill
                d1.at[bill_idx, "_Cleared_By"]     += (
                    (", " if d1.at[bill_idx, "_Cleared_By"] else "") + "Suspense"
                )
                d1.at[bill_idx, "_Cleared_Ref"] += (
                    (", " if d1.at[bill_idx, "_Cleared_Ref"] else "") +
                    "+".join(cleared_refs)
                )
                if d1.at[bill_idx, "_Remaining"] <= CLEARING_ROUNDING_THRESHOLD:
                    d1.at[bill_idx, "_Status"] = BillStatus.CLEARED
                else:
                    d1.at[bill_idx, "_Status"] = BillStatus.PARTIALLY_CLEARED

                statement.append({
                    "Vertical":         vertical,
                    "Client":           rep["resolved_name"],
                    "Bill_Ref":         bill.get("Ref. No.", ""),
                    "Bill_Date":        bill.get("Date", ""),
                    "Bill_Amount":      bill.get("Pending Amount", ""),
                    "Cleared_By":       "Suspense",
                    "Cleared_Ref":      "+".join(cleared_refs),
                    "Cleared_Date":     rep["date"],
                    "Cleared_Amount":   round(apply_to_bill, 2),
                    "Remaining_After":  round(d1.at[bill_idx, "_Remaining"], 2),
                    "Match_Confidence": round(rep["confidence"], 4),
                    "Match_Method":     rep["method"] + "_CROSS_BRANCH",
                    "Narration":        rep["narration"],
                })

                remaining_pool -= apply_to_bill

            # Unmatched groups were already logged in Step 2 — don't double-log.

        return d1, s3, statement, kb_gaps

    # ── Core matching and application ─────────────────────────────────────────

    def _find_and_apply(
        self,
        d1,
        resolved_name,
        pay_amount,
        vertical,
        prefix,
        source_type,
        source_ref,
        source_date,
        raw_text,
        narration,
        confidence,
        method,
        statement,
        kb_gaps,
        suspense_idx=None,
        s3=None,
    ) -> bool:
        """
        Find matching debtors bills for a resolved client + amount,
        apply payment FIFO, update d1 in-place.

        Returns True if any match was applied.
        """

        # Build candidate bill pool — Option A: branch-exact first, base fallback.
        #
        # Step 1: if the resolver returned a branch-specific name (e.g.
        #   'INDUSIND BANK LIMITED_JAIPUR') AND that exact name has open bills,
        #   restrict candidates to that branch only.
        #   → JAIPUR payment clears JAIPUR bills, not AGRA/KURNOOL.
        #
        # Step 2: if no open bills exist for that exact branch (fully cleared, or
        #   resolver returned a branch-agnostic name like 'INDUSIND BANK LIMITED'),
        #   fall back to all branches matching the base name.
        #   → No match is lost; existing behaviour preserved.
        resolved_base = _normalise_base(_extract_client_base(resolved_name))
        resolved_branch = _extract_branch(resolved_name)

        if resolved_branch:
            # Resolver gave us a branch — try exact match first
            branch_exact = d1[
                (d1["_Remaining"] > 0) &
                (d1["Party's Name"] == resolved_name)
            ]
            if not branch_exact.empty:
                candidates = branch_exact
            else:
                # Branch exhausted or name variant mismatch — fall back to base
                candidates = d1[
                    (d1["_Remaining"] > 0) &
                    (d1["_base_name"] == resolved_base)
                ]
        else:
            # No branch in resolved name — use base match across all branches
            candidates = d1[
                (d1["_Remaining"] > 0) &
                (d1["_base_name"] == resolved_base)
            ]

        # For Credit vertical with known prefix, filter by prefix
        if vertical == CREDIT_VERTICAL and prefix:
            prefixed = candidates[
                candidates["Ref. No."].apply(
                    lambda r: extract_ref_prefix(str(r)) == prefix
                )
            ]
            if not prefixed.empty:
                candidates = prefixed
            # If no prefixed candidates found, fall through to all candidates
            # (edge case: prefix determined but bills all in other prefix)

        if candidates.empty:
            # Client resolved but no open bills — goes to unresolvable
            kb_gaps.append({
                "Vertical":          vertical,
                "Source":            source_type,
                "Date":              source_date,
                "Raw_Text":          raw_text,
                "Amount":            pay_amount,
                "Narration":         narration,
                "Method":            method,
                "Suggested_Client":  resolved_name,
                "Action_Required":   GapReason.NO_OPEN_BILLS,
            })
            return False

        # Sort candidates FIFO (oldest bill first)
        candidates = candidates.copy()  # avoid SettingWithCopyWarning
        candidates["_date_parsed"] = pd.to_datetime(
            candidates["Date"], format="%d-%b-%Y", errors="coerce"
        )
        nat_mask = candidates["_date_parsed"].isna()
        if nat_mask.any():
            print(
                f"  ⚠  {nat_mask.sum()} bill(s) for '{resolved_name}' have unparseable dates "
                f"and will be queued last in FIFO. Check debtors file for date format issues."
            )
        candidates = candidates.sort_values("_date_parsed", na_position="last")

        # Find best amount match within tolerance
        matched_any = False
        remaining_pay = pay_amount

        for bill_idx, bill in candidates.iterrows():
            if remaining_pay <= 0:
                break

            bill_remaining = d1.at[bill_idx, "_Remaining"]
            if bill_remaining <= 0:
                continue

            # Determine applied amount
            if _within_tolerance(bill_remaining, remaining_pay):
                # Full match (within tolerance)
                # #30: Cap applied at bill_remaining to prevent money vanishing
                applied = min(remaining_pay, bill_remaining)
                underpayment_flagged = False
            elif remaining_pay > bill_remaining and not _within_tolerance(bill_remaining, remaining_pay):
                # Payment larger than bill — apply full bill, carry remainder
                applied = bill_remaining
                underpayment_flagged = False
            elif remaining_pay < bill_remaining:
                # Only treat as underpayment if the payment is at least MIN_PARTIAL_RATIO
                # of the bill. Payments below this threshold are instalment tranches —
                # leave them pending so Step 2b aggregation can group them.
                if remaining_pay < bill_remaining * MIN_PARTIAL_RATIO:
                    continue
                applied = remaining_pay
                underpayment_flagged = True
            else:
                continue

            # R5: Single bill-update path (replaces two duplicated blocks)
            d1.at[bill_idx, "_Remaining"]      = max(0, bill_remaining - applied)
            d1.at[bill_idx, "_Cleared_Amount"] = (
                d1.at[bill_idx, "_Cleared_Amount"] + applied
            )
            d1.at[bill_idx, "_Cleared_By"] = (
                d1.at[bill_idx, "_Cleared_By"] +
                (", " if d1.at[bill_idx, "_Cleared_By"] else "") +
                source_type
            )
            d1.at[bill_idx, "_Cleared_Ref"] = (
                d1.at[bill_idx, "_Cleared_Ref"] +
                (", " if d1.at[bill_idx, "_Cleared_Ref"] else "") +
                str(source_ref)
            )
            # Update status
            if d1.at[bill_idx, "_Remaining"] <= CLEARING_ROUNDING_THRESHOLD:
                d1.at[bill_idx, "_Status"] = BillStatus.CLEARED
            else:
                d1.at[bill_idx, "_Status"] = BillStatus.PARTIALLY_CLEARED

            # Update suspense remaining if applicable
            if suspense_idx is not None and s3 is not None:
                s3.at[suspense_idx, "_Applied"]   += applied
                s3.at[suspense_idx, "_Remaining"] = max(
                    0, s3.at[suspense_idx, "_Remaining"] - applied
                )
                if s3.at[suspense_idx, "_Remaining"] <= CLEARING_ROUNDING_THRESHOLD:
                    s3.at[suspense_idx, "_Status"] = SuspenseStatus.FULLY_APPLIED
                else:
                    s3.at[suspense_idx, "_Status"] = SuspenseStatus.PARTIALLY_APPLIED

            statement.append({
                "Vertical":          vertical,
                "Client":            bill.get("Party's Name", resolved_name),
                "Bill_Ref":          bill.get("Ref. No.", ""),
                "Bill_Date":         bill.get("Date", ""),
                "Bill_Amount":       bill.get("Pending Amount", ""),
                "Cleared_By":        source_type,
                "Cleared_Ref":       source_ref,
                "Cleared_Date":      source_date,
                "Cleared_Amount":    round(applied, 2),
                "Remaining_After":   round(d1.at[bill_idx, "_Remaining"], 2),
                "Match_Confidence":  round(confidence, 4),
                "Match_Method":      method,
                "Narration":         narration,
            })

            if underpayment_flagged:
                kb_gaps.append({
                    "Vertical":          vertical,
                    "Source":            source_type,
                    "Date":              source_date,
                    "Raw_Text":          raw_text,
                    "Amount":            applied,
                    "Narration":         narration,
                    "Method":            method,
                    "Suggested_Client":  resolved_name,
                    "Action_Required":   GapReason.AMOUNT_MISMATCH,
                })

            remaining_pay -= applied
            matched_any    = True

        # If payment amount not matched to any bill
        if not matched_any:
            kb_gaps.append({
                "Vertical":          vertical,
                "Source":            source_type,
                "Date":              source_date,
                "Raw_Text":          raw_text,
                "Amount":            pay_amount,
                "Narration":         narration,
                "Method":            method,
                "Suggested_Client":  resolved_name,
                "Action_Required":   GapReason.AMOUNT_MISMATCH,
            })

        return matched_any

    # ── Output builders ───────────────────────────────────────────────────────

    def _build_updated_debtors(self, d1: pd.DataFrame) -> pd.DataFrame:
        """Build updated debtors output with status columns."""
        out = d1.copy()
        out = out.rename(columns={
            "_Remaining":     "Remaining_Amount",
            "_Status":        "Status",
            "_Cleared_Amount":"Cleared_Amount",
            "_Cleared_By":    "Cleared_By",
            "_Cleared_Ref":   "Cleared_Ref",
        })
        # Drop internal working columns
        drop_cols = [c for c in out.columns if c.startswith("_")]
        out = out.drop(columns=drop_cols, errors="ignore")

        # Reorder: status columns at front after key identifiers
        priority = [
            "Vertical", "Status", "Date", "Ref. No.", "Party's Name",
            "Pending Amount", "Remaining_Amount", "Cleared_Amount",
            "Cleared_By", "Cleared_Ref", "Days Overdue", "Days Bucket",
            "Amount Bucket", "State",
        ]
        cols = [c for c in priority if c in out.columns] + \
               [c for c in out.columns if c not in priority]
        return out[cols]

    def _build_updated_suspense(self, s3: pd.DataFrame, credit_col: str) -> pd.DataFrame:
        """Build updated suspense output with remaining amounts."""
        out = s3.copy()
        out = out.rename(columns={
            "_Applied":   "Applied_Amount",
            "_Remaining": "Remaining_Amount",
            "_Status":    "Status",
        })
        drop_cols = [c for c in out.columns if c.startswith("_")]
        out = out.drop(columns=drop_cols, errors="ignore")

        priority = [
            "Vertical", "Status", "Date", credit_col,
            "Applied_Amount", "Remaining_Amount",
            "Particulars", "Narration", "Vch Type",
        ]
        cols = [c for c in priority if c in out.columns] + \
               [c for c in out.columns if c not in priority]
        return out[cols]


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-VERTICAL RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def run_all_verticals(
    debtors_dfs:  dict,   # {saved_name: df}
    receipts_dfs: dict,   # {saved_name: df}
    suspense_dfs: dict,   # {saved_name: df}
    kb_path:      str = str(DATA_DIR / "master_client_knowledge_base.json"),
) -> dict:
    """
    Run reconciliation for all verticals and aggregate results.

    Vertical matching:
        Each debtors file has a 'Vertical' column.
        Receipts and suspense are matched to debtors by the same Vertical value.

    Returns:
        {
            'by_vertical': {vertical: result_dict},
            'combined': {
                'statement', 'updated_debtors', 'updated_suspense',
                'kb_gaps', 'unresolvable'
            },
            'summary': list of per-vertical summary dicts
        }
    """
    # Combine all dataframes
    all_debtors  = pd.concat(debtors_dfs.values(),  ignore_index=True) if debtors_dfs  else pd.DataFrame()
    all_receipts = pd.concat(receipts_dfs.values(), ignore_index=True) if receipts_dfs else pd.DataFrame()
    all_suspense = pd.concat(suspense_dfs.values(), ignore_index=True) if suspense_dfs else pd.DataFrame()

    if all_debtors.empty:
        return {}

    # R9: Build validator and bridge once, inject into Reconciler
    validator = None
    kb_bridge = {}
    if kb_path and Path(kb_path).exists():
        validator = Validator(kb_path)
        kb_bridge = _build_kb_bridge(kb_path)

    rec = Reconciler(kb_path=kb_path, validator=validator, kb_bridge=kb_bridge)

    by_vertical = {}
    summaries   = []

    verticals = all_debtors["Vertical"].unique().tolist()

    for vertical in verticals:
        d_vert = all_debtors[all_debtors["Vertical"] == vertical].copy()

        r_vert = all_receipts[all_receipts["Vertical"] == vertical].copy() \
                 if not all_receipts.empty and "Vertical" in all_receipts.columns \
                 else pd.DataFrame()

        s_vert = all_suspense[all_suspense["Vertical"] == vertical].copy() \
                 if not all_suspense.empty and "Vertical" in all_suspense.columns \
                 else pd.DataFrame()

        if r_vert.empty and s_vert.empty:
            receipt_verticals = all_receipts["Vertical"].unique().tolist() if not all_receipts.empty and "Vertical" in all_receipts.columns else []
            suspense_verticals = all_suspense["Vertical"].unique().tolist() if not all_suspense.empty and "Vertical" in all_suspense.columns else []
            print(
                f"\n⚠  VERTICAL MISMATCH — '{vertical}' has {len(d_vert)} debtors bills but "
                f"no matching receipts or suspense.\n"
                f"   Receipt verticals detected:  {receipt_verticals}\n"
                f"   Suspense verticals detected: {suspense_verticals}\n"
                f"   Check that the uploaded files are for the correct entity."
            )
            # Record the skipped vertical in results so app.py can surface it
            by_vertical[vertical] = {
                "statement": pd.DataFrame(),
                "updated_debtors": d_vert.copy(),
                "updated_suspense": pd.DataFrame(),
                "kb_gaps": pd.DataFrame(),
                "advance_payments": pd.DataFrame(),
                "unresolvable": pd.DataFrame(),
                "summary": {
                    "vertical": vertical,
                    "skipped": True,
                    "skip_reason": "no_receipts_or_suspense",
                    "debtors_bills": len(d_vert),
                    "bills_cleared": 0,
                    "bills_partial": 0,
                    "bills_open": len(d_vert),
                    "total_debtors_initial": round(d_vert["Pending Amount"].apply(lambda x: pd.to_numeric(x, errors="coerce")).sum(), 2),
                    "total_cleared": 0,
                    "total_outstanding": round(d_vert["Pending Amount"].apply(lambda x: pd.to_numeric(x, errors="coerce")).sum(), 2),
                    "suspense_entries": 0,
                    "suspense_applied": 0,
                    "suspense_remaining": 0,
                    "statement_entries": 0,
                    "kb_gaps": 0,
                    "advance_payments": 0,
                    "unresolvable": 0,
                },
            }
            summaries.append(by_vertical[vertical]["summary"])
            continue

        result = rec.reconcile(d_vert, r_vert, s_vert, vertical)
        by_vertical[vertical] = result
        summaries.append(result["summary"])

    if not by_vertical:
        return {}

    # Combine all vertical results
    def _concat(key):
        frames = [v[key] for v in by_vertical.values() if not v[key].empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    combined = {
        "statement":          _concat("statement"),
        "updated_debtors":    _concat("updated_debtors"),
        "updated_suspense":   _concat("updated_suspense"),
        "kb_gaps":            _concat("kb_gaps"),
        "advance_payments":   _concat("advance_payments"),
        "unresolvable":       _concat("unresolvable"),
    }

    return {
        "by_vertical": by_vertical,
        "combined":    combined,
        "summaries":   summaries,
    }