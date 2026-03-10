"""
backend/config.py - Shared constants for the Account Tracker system.
All rates, thresholds, and column pattern lists live here.

This module is a leaf dependency — imported by everything, imports nothing
project-internal. Only addition vs the original: DATA_DIR path constant.
"""

from pathlib import Path

# ── Data Directory ─────────────────────────────────────────────────────────────
# All JSON reference files (KB, PNM, state_mapping) live in data/
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ── Tax Rates ──────────────────────────────────────────────────────────────────
GST_RATE   = 0.18   # 18% total GST
CGST_RATE  = 0.09   # 9%  Central GST  (intrastate)
SGST_RATE  = 0.09   # 9%  State GST    (intrastate)
IGST_RATE  = 0.18   # 18% Integrated GST (interstate)
TDS_RATE   = 0.10   # 10% TDS deduction

# ── Matching ───────────────────────────────────────────────────────────────────
MATCH_TOLERANCE = 0.015   # 1.5% tolerance for amount matching

# ── Clearing ───────────────────────────────────────────────────────────────────
# If remaining amount on a bill/suspense entry is ≤ this after application,
# treat as fully CLEARED.  Absorbs sub-rupee rounding differences.
CLEARING_ROUNDING_THRESHOLD = 1.0   # ₹1

# ── Reconciler Confidence ──────────────────────────────────────────────────────
HIGH_CONF              = 0.88   # auto-accept match
LOW_CONF               = 0.50   # below this → unresolvable
BRANCH_FUZZY_THRESHOLD = 0.82   # branch-aware fuzzy minimum score

# ── Validator ──────────────────────────────────────────────────────────────────
FUZZY_MATCH_THRESHOLD = 0.82    # SequenceMatcher minimum for fuzzy accept

# ── Parser ─────────────────────────────────────────────────────────────────────
SUSPENSE_HEADER_SCAN_ROWS = 15  # Rows to scan for header in suspense files
RECEIPT_HEADER_SCAN_ROWS  = 15  # Rows to scan for header in receipt files
DEBTORS_HEADER_SCAN_ROWS  = 20  # Rows to scan for header in debtors files

# ── Ingestor (reserved — not yet ported from original pipeline) ────────────────
HEADER_SCAN_ROWS   = 30   # How many rows to scan when looking for header
MIN_HEADER_SCORE   = 3    # Minimum keyword hits to accept a row as header
CONTENT_SAMPLE     = 100  # Rows to sample for content-based column detection

# Column name patterns for smart detection
# Each key maps to a list of known variations for that field type.
# Used by the ingestor's fuzzy column detector.

COLUMN_PATTERNS = {

    # ── Invoice ID ─────────────────────────────────────────────────────────────
    "invoice_id": [
        "voucher no", "voucher no.", "voucher number",
        "invoice no", "invoice no.", "invoice number", "invoice id",
        "inv no", "inv_no", "inv id",
        "bill no", "bill no.", "bill number", "bill id",
        "ref no", "ref no.", "reference no", "reference number",
        "doc no", "document no", "sr", "sr no", "sr.",
    ],

    # ── Date ───────────────────────────────────────────────────────────────────
    "date": [
        "date", "invoice date", "bill date",
        "transaction date", "txn date", "trans date",
        "posting date", "doc date", "entry date",
        "value date", "dt",
    ],

    # ── Client / Party ─────────────────────────────────────────────────────────
    "client": [
        "particulars", "particulars 1", "particulars 2",
        "party", "party name",
        "client", "client name",
        "customer", "customer name",
        "company", "company name",
        "name", "account name",
        "vendor", "buyer", "firm",
        "description", "narration",
    ],

    # ── Base Amount (before tax) ───────────────────────────────────────────────
    "base_amount": [
        "value", "taxable value", "taxable amount",
        "base amount", "basic amount",
        "sales", "sale value",
        "without gst amt", "without gst amount",
        "net amount", "net value",
    ],

    # ── IGST ───────────────────────────────────────────────────────────────────
    "igst": [
        "igst output", "igst amount", "igst",
        "integrated gst", "integrated tax",
    ],

    # ── CGST ───────────────────────────────────────────────────────────────────
    "cgst": [
        "cgst output", "cgst amount", "cgst",
        "central gst", "central tax",
    ],

    # ── SGST ───────────────────────────────────────────────────────────────────
    "sgst": [
        "sgst output", "sgst amount", "sgst",
        "state gst", "state tax",
    ],

    # ── TDS ────────────────────────────────────────────────────────────────────
    "tds": [
        "tds", "tds amount", "tds deduction",
        "tax deducted", "tax deducted at source",
        "tds deducted",
    ],

    # ── Gross Total (base + gst, before TDS) ──────────────────────────────────
    "gross_total": [
        "gross total", "gross amount", "gross value",
        "total amount", "total value",
        "invoice amount", "bill amount",
        "gst amt", "gst amount", "gst amt.",
    ],

    # ── Final Amount (base + gst - tds) = reconciliation amount ───────────────
    "final_amount": [
        "amt. to be received", "amt to be received",
        "amount to be received", "amount receivable",
        "net payable", "net receivable",
        "receivable amount", "payable amount",
        "final amount", "net amount payable",
        "amt. to be recieved", "amt to be recieved",
    ],

    # ── Suspense: Credit Amount ────────────────────────────────────────────────
    "credit": [
        "credit", "cr", "credit amount",
        "deposit", "deposit amt", "deposite amt",
        "deposite amount", "deposit amount",
        "received", "amount received",
        "payment received", "receipt",
        "inward", "inflow",
    ],

    # ── Suspense: Bank Reference ───────────────────────────────────────────────
    "bank_ref": [
        "utr", "utr no", "utr number",
        "reference", "ref no", "reference no",
        "transaction id", "txn id", "tran id",
        "cheque no", "chq no",
        "neft ref", "imps ref", "rtgs ref",
    ],
}

# ── Cancelled Row Detection ────────────────────────────────────────────────────
# If Particulars/Description contains any of these → row is cancelled, drop it
CANCELLED_KEYWORDS = [
    "(cancelled)",
    "(cancelled )",
    "cancelled",
    "cancel",
    "(void)",
    "void",
    "(deleted)",
]

# ── Header Detection Keywords ──────────────────────────────────────────────────
# Used when scanning for the real header row in messy files
HEADER_KEYWORDS = [
    "date", "particulars", "voucher", "invoice",
    "amount", "value", "credit", "debit",
    "description", "narration", "balance",
    "gst", "igst", "tds", "total",
]
