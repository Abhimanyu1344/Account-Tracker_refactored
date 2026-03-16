# CLAUDE.md — Account Tracker: Project Bible
> This file is the permanent source of truth for all Claude Code sessions on this project.
> Read this entire file before doing anything. Then read FIX_PLAN.md for current work.
> **One task per session. Show diff. Wait for confirmation before writing any file.**

---

## 1. Project Identity

This is a financial reconciliation system. It matches incoming payments (from Receipt Register and Suspense ledger files exported from Tally) against outstanding invoices (from Debtors Ageing files, also from Tally) across five business verticals. The output is an Excel report showing which bills were cleared, which payments could not be matched, and which require human investigation.

**The stakes are real financial records. A wrong match is worse than no match.**
An unmatched payment surfaces in KB Gaps for human review. A wrong match corrupts the debtors ledger silently. Always prefer surfacing uncertainty over applying a guess.

---

## 2. Architecture — Module Responsibility Map

Each module has exactly one job. Do not move logic between modules. Do not add domain logic to a module that does not own it.

| File | Owns | Does NOT own |
|---|---|---|
| `backend/config.py` | All constants, thresholds, rates, scan row counts | Any logic, any imports from this project |
| `backend/common.py` | Shared utilities (`_fuzzy`, `_normalise*`, `_parse_date`, `_clean_tally_text`), all Enums (`BillStatus`, `SuspenseStatus`, `GapReason`, `UnresolvableReason`), `ResolutionResult` dataclass | Any domain logic, any parsing, any reconciliation |
| `backend/parsers/debtors.py` | Parsing Debtors Ageing Excel files, PNM (`standardise_party_name`), state mapping (`map_state`), bucketing (`bucket_days`, `bucket_amount`), `extract_ref_prefix` | Anything to do with receipts, suspense, or reconciliation |
| `backend/parsers/bank_books.py` | Parsing Receipt Register and Suspense Excel files, vertical detection (`detect_vertical`, `detect_vertical_from_row1`, `detect_vertical_from_filename`), `merge_tally_rows`, `generate_file_name` | Anything to do with debtors or reconciliation |
| `backend/reconciler/resolver.py` | Answering "who is this payment from?" — `Validator`, `ClientResolver`, `_parse_neft_client`, `_is_non_client`, `_clean_candidate`, `_build_kb_bridge`, `_extract_client_base`, `_extract_branch` | Bill matching, FIFO logic, amount application |
| `backend/reconciler/engine.py` | Answering "which bill does this clear?" — `Reconciler`, `run_all_verticals`, `_find_and_apply`, `_apply_direct_receipts`, `_apply_suspense`, `_apply_suspense_aggregated`, `_build_updated_debtors`, `_build_updated_suspense` | Client name resolution, narration parsing |
| `backend/output/presentation.py` | Column renaming, column ordering, display logic for all five output sheets, translation functions (`translate_*`) | Data generation, reconciliation logic |
| `backend/output/report_builder.py` | Building the styled Excel workbook from reconciliation output DataFrames | Any reconciliation logic, any presentation decisions |
| `frontend/app.py` | Streamlit UI, session state management, file upload handling, triggering reconciliation, rendering results | Any business logic whatsoever |
| `data/*.json` | Reference data only — KB, PNM, state mapping | Never modified by code at runtime (read-only inputs) |

---

## 3. Import Dependency Chain

The import direction flows strictly downward. Nothing imports "up" this chain. Never add an import that violates this hierarchy.

```
config.py
    ↓
common.py
    ↓
parsers/debtors.py   parsers/bank_books.py
         ↘               ↙
      reconciler/resolver.py
              ↓
      reconciler/engine.py
              ↓
    output/presentation.py
    output/report_builder.py
              ↓
         frontend/app.py
```

**engine.py imports from resolver.py and parsers/debtors.py — never the reverse.**
**resolver.py imports from parsers/debtors.py — never the reverse.**
**common.py imports from config.py only — never from any other project module.**

---

## 4. Critical Data Contracts

### 4.1 The Debtors Working DataFrame (d1)

`d1` is created at the start of `Reconciler.reconcile()` and mutated in-place throughout Steps 1, 2, and 2b. It is never reassigned — only mutated via `d1.at[idx, col]`.

**Columns that must exist before Step 1 begins:**

| Column | Type | Set by |
|---|---|---|
| `Vertical` | str | debtors parser |
| `Date` | str (DD-MMM-YYYY) | debtors parser |
| `Ref. No.` | str | debtors parser |
| `Party's Name` | str | debtors parser |
| `Pending Amount` | float | debtors parser |
| `Days Overdue` | int | debtors parser |
| `Days Bucket` | str | debtors parser |
| `Amount Bucket` | str | debtors parser |
| `State` | str | debtors parser |
| `_Remaining` | float | `reconcile()` init |
| `_Status` | BillStatus | `reconcile()` init |
| `_Cleared_Amount` | float | `reconcile()` init |
| `_Cleared_By` | str | `reconcile()` init |
| `_Cleared_Ref` | str | `reconcile()` init |
| `_base_name` | str | `reconcile()` init |

**Never add a column to d1 whose name starts with `_` unless it is a temporary working column meant to be dropped in `_build_updated_debtors`.** See Section 5.

### 4.2 The Suspense Working DataFrame (s3)

`s3` is created at the start of `Reconciler.reconcile()` and mutated in-place throughout Steps 2 and 2b.

**Working columns initialised in `reconcile()`:**

| Column | Type | Set by |
|---|---|---|
| `_Remaining` | float | `reconcile()` init (from credit column) |
| `_Applied` | float | `reconcile()` init |
| `_Status` | SuspenseStatus | `reconcile()` init |

**Source columns passed through from parser (must be present):**

`Date`, `Particulars`, `Narration`, `Vch Type`, `Vch No.` (or `Vch No`), `Credit`, `Debit`, `Vertical`

### 4.3 Output Row Schemas

Every row appended to `statement`, `kb_gaps`, or `unresolvable` lists must contain these exact keys. Do not add or remove keys from these schemas without updating `presentation.py` and `report_builder.py` simultaneously.

**Statement row (minimum required keys):**
`Vertical`, `Client`, `Bill_Ref`, `Bill_Date`, `Bill_Amount`, `Cleared_By`, `Cleared_Ref`, `Cleared_Date`, `Cleared_Amount`, `Remaining_After`, `Match_Confidence`, `Match_Method`, `Narration`

**KB Gaps row (minimum required keys):**
`Vertical`, `Source`, `Date`, `Raw_Text`, `Amount`, `Narration`, `Method`, `Suggested_Client`, `Action_Required`

**Unresolvable row (minimum required keys):**
`Vertical`, `Source`, `Date`, `Raw_Text`, `Amount`, `Reason`, `Narration`

---

## 5. The Underscore Prefix Rule — Read This Carefully

In `_build_updated_debtors()` and `_build_updated_suspense()`, this cleanup pattern runs:

```python
drop_cols = [c for c in out.columns if c.startswith('_')]
out = out.drop(columns=drop_cols, errors='ignore')
```

**This means:**
- Any column added to `d1` or `s3` whose name starts with `_` WILL be dropped from the final output DataFrames. This is correct and intentional for temporary working columns.
- Any column added to `d1` or `s3` whose name does NOT start with `_` will SURVIVE into the output and appear in the Excel report. This may or may not be intended.
- When a task requires adding a cache or working column to `d1` or `s3`, always prefix it with `_` so it is cleaned up automatically.
- If a task explicitly requires a column to survive to output, it must NOT be prefixed with `_`, and `presentation.py` and `report_builder.py` must be updated to handle it.

---

## 6. Domain Constants and Enums — Single Sources of Truth

**Never define a constant or enum anywhere except its designated home file.**

### Constants — all live in `backend/config.py`
- `MATCH_TOLERANCE = 0.015` — 1.5% tolerance for amount matching
- `CLEARING_ROUNDING_THRESHOLD = 1.0` — ₹1 rounding absorber
- `HIGH_CONF = 0.88` — auto-accept match threshold
- `LOW_CONF = 0.50` — below this, route to unresolvable
- `BRANCH_FUZZY_THRESHOLD = 0.82` — branch-aware fuzzy minimum
- `FUZZY_MATCH_THRESHOLD = 0.82` — KB validator fuzzy minimum
- `DATA_DIR` — path to `data/` directory

### Enums — all live in `backend/common.py`
- `BillStatus` — `OPEN`, `CLEARED`, `PARTIALLY_CLEARED`
- `SuspenseStatus` — `OPEN`, `FULLY_APPLIED`, `PARTIALLY_APPLIED`
- `GapReason` — `AMOUNT_MISMATCH`, `UNKNOWN_CLIENT`, `NO_OPEN_BILLS`
- `UnresolvableReason` — `NARRATION_UNPARSEABLE`, `NON_CLIENT_NARRATION`, `CPV_CPD_UNDETERMINED`, `NO_NARRATION`

### Dataclasses — live in `backend/common.py`
- `ResolutionResult` — fields: `resolved_name`, `confidence`, `method`, `raw_text`, `suggestion`

### Reference Data — live in `data/`
- `master_client_knowledge_base.json` — KB client list with keywords
- `party_name_map.json` — PNM raw → standardised name map
- `state_mapping.json` — party name → state map

**Never modify JSON files in `data/` programmatically. They are read-only inputs.**

---

## 7. Key Invariants — Must Always Be True After Any Change

1. **FIFO ordering is sacred.** Bills are always sorted oldest-first before matching. Payments are always sorted oldest-first before application. This must be preserved in `_find_and_apply`, `_apply_direct_receipts`, `_apply_suspense`, and `_apply_suspense_aggregated`.

2. **d1 mutations are in-place only.** Never reassign `d1`. Always use `d1.at[bill_idx, col] = value`. The `_find_and_apply` method receives d1 by reference and mutates it — this is intentional.

3. **s3 mutations are in-place only.** Same rule as d1. Never reassign `s3`.

4. **A payment that is applied must always generate a statement row.** No silent application without an audit trail.

5. **A resolved client with no matching bill amount goes to `kb_gaps`, not `unresolvable`.** `kb_gaps` = client known, amount problem. `unresolvable` = client unknown or unparseable.

6. **The Vertical column must be present in all three DataFrames** (debtors, receipts, suspense) before `run_all_verticals` filters them. The vertical label must be identical across all three — case-sensitive string match. Any mismatch causes the entire vertical to be silently skipped.

7. **`_build_updated_debtors` and `_build_updated_suspense` must never be modified** without re-reading the underscore prefix rule (Section 5) and verifying that all working columns are correctly dropped.

8. **`run_all_verticals` function signature must not change.** `app.py` calls it with `(debtors_dfs, receipts_dfs, suspense_dfs, kb_path)` and expects `{'by_vertical': ..., 'combined': ..., 'summaries': ...}`.

9. **`Reconciler.reconcile()` function signature must not change.** Called with `(debtors_df, receipts_df, suspense_df, vertical)`.

10. **Session state in `app.py` must always be initialised through `_DEFAULTS`.** Never set a new `st.session_state` key without adding it to the `_DEFAULTS` dict first.

---

## 8. Vertical Labels — Canonical Values

These are the only valid vertical strings used throughout the system. Any new vertical requires updates to ENTITY_MAP and VERTICAL_MAP in `bank_books.py`, the debtors upload handler in `app.py`, and `data/state_mapping.json`.

| Label | Entity |
|---|---|
| `Credit` | Greenfinch Tech Process |
| `Legal` | Greenfinch Legal Services |
| `GFGC` | Greenfinch Global Consulting |
| `GREEC` | Greenfinch Real Estate Engineers & Consultants |
| `GREECPL` | Greenfinch Real Estate Eng. & Con. (P) Ltd. (H.O.) |

`CREDIT_VERTICAL = "Credit"` is the only vertical that uses CPV/CPD prefix logic in `engine.py`. Do not apply prefix logic to any other vertical.

---

## 9. Global Constraints for Every Claude Code Session

These apply to every task, every session, with no exceptions.

**Workflow:**
- Read this file fully before starting.
- Read `FIX_PLAN.md` and identify the first task with `Status: PENDING`.
- Implement exactly that one task. Nothing else.
- Show the complete diff of every file you intend to modify before writing any file.
- Wait for explicit confirmation before writing.
- Do not proceed to the next task in the same session.

**Scope:**
- Do not modify any file not listed in the current task's `Files` field.
- Do not rename any existing function, variable, column name, or enum value unless the task explicitly requires it.
- Do not add comments, docstrings, or formatting changes to code outside the changed lines.
- Do not reorganise imports unless an import is required by the task.
- Do not add logging or print statements unless the task explicitly requires it.

**Never touch without explicit task instruction:**
- `data/*.json` — reference data, read-only
- `backend/common.py` — only touch when a task explicitly targets an enum or utility here
- `backend/config.py` — only touch when a task explicitly targets a constant here
- `backend/output/report_builder.py` — only touch when a task explicitly targets output schema
- Any function signature listed in Invariant 8 or 9 above

**Never do:**
- Implement multiple tasks in one session
- Refactor code that works correctly and is not part of the current task
- Add test files, mock data, or example scripts without explicit instruction
- Change the Excel sheet names or column names in the final output without updating `presentation.py` and `report_builder.py` simultaneously

---

## 10. Current Work

All 24 fixes from the initial reconciliation engine hardening sprint are complete.

For any new feature or bug fix, create a new task file (e.g. `TASK_NAME.md`) 
at the project root following the same pattern as the completed fix plan:
- One task per session
- Files scope declared explicitly  
- Exact before/after code for complex changes
- Dependency order declared upfront

Read this file (CLAUDE.md) before reading any task file.