import io
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from backend.output.report_builder import build_reconciliation_report
from backend.parsers.bank_books import detect_vertical, generate_file_name
from backend.parsers.debtors import parse_debtor_file
from backend.parsers.bank_books import parse_suspense_file
from backend.parsers.bank_books import parse_receipt_file
from backend.reconciler.engine import run_all_verticals
from backend.output.presentation import (
    apply_statement_presentation,
    apply_debtors_presentation,
    apply_kb_gaps_presentation,
    apply_unresolvable_presentation,
    apply_suspense_presentation,
)

st.set_page_config(page_title="Account Tracker", layout="wide")

# ─────────────────────────────────────────────────────────────────────────────
# Session State
# ─────────────────────────────────────────────────────────────────────────────
_DEFAULTS = {
    "uploaded_files_log": [],
    "show_upload_panel":  False,
    "debtor_dfs":         {},
    "suspense_dfs":       {},
    "receipt_dfs":        {},
    "active_section":     None,
    "recon_results":      None,   # dict returned by run_all_verticals
    "recon_ran":          False,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _fmt_inr(val) -> str:
    try:
        return f"₹{float(val):,.2f}"
    except Exception:
        return "₹0.00"

def _to_excel(sheets: dict) -> bytes:
    """Write multiple DataFrames to an Excel workbook in memory."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            if df is not None and not df.empty:
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return buf.getvalue()

def _status_colour(val) -> str:
    colours = {
        "CLEARED":           "background-color:#d4edda; color:#155724;",
        "PARTIALLY_CLEARED": "background-color:#fff3cd; color:#856404;",
        "OPEN":              "background-color:#f8d7da; color:#721c24;",
        "FULLY_APPLIED":     "background-color:#d4edda; color:#155724;",
        "PARTIALLY_APPLIED": "background-color:#fff3cd; color:#856404;",
    }
    key = val.value.upper() if hasattr(val, 'value') else str(val).upper()
    return colours.get(key, "")


def _conf_colour(val) -> str:
    try:
        v = float(val)
        if v >= 0.88: return "background-color:#d4edda;"
        if v >= 0.70: return "background-color:#fff3cd;"
        return "background-color:#f8d7da;"
    except Exception:
        return ""


def _has_df(store: dict) -> bool:
    return any(v is not None and not v.empty for v in store.values())


def _resolve_display_vertical(row) -> str:
    if row["Vertical"] == "Credit":
        ref = str(row.get("Ref. No.", "")).upper()
        if "CPV" in ref:   return "CPV"
        if "CPD" in ref or "GFTP" in ref: return "CPD"
    return row["Vertical"]

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📁 File Portal")
    st.divider()

    if st.button("⬆️ Upload Files", width="stretch", type="primary"):
        st.session_state.show_upload_panel = not st.session_state.show_upload_panel

    has_debtors  = _has_df(st.session_state.debtor_dfs)
    has_suspense = _has_df(st.session_state.suspense_dfs)
    has_receipts = _has_df(st.session_state.receipt_dfs)
    has_any      = has_debtors or has_suspense or has_receipts
    has_all      = has_debtors and has_suspense and has_receipts

    if has_any:
        st.divider()
        st.caption("📌 Navigate to")

        if has_debtors:
            is_active = st.session_state.active_section == "Debtors"
            if st.button("📊 Debtors", width="stretch",
                         type="primary" if is_active else "secondary",
                         key="nav_debtors"):
                st.session_state.active_section = "Debtors"
                st.rerun()

        if has_suspense:
            is_active = st.session_state.active_section == "Suspense"
            if st.button("🔄 Suspense", width="stretch",
                         type="primary" if is_active else "secondary",
                         key="nav_suspense"):
                st.session_state.active_section = "Suspense"
                st.rerun()

        if has_receipts:
            is_active = st.session_state.active_section == "Receipt"
            if st.button("🧾 Receipt", width="stretch",
                         type="primary" if is_active else "secondary",
                         key="nav_receipt"):
                st.session_state.active_section = "Receipt"
                st.rerun()

        st.divider()

        # Reconciliation nav — always visible once any data is present
        is_active = st.session_state.active_section == "Reconciliation"
        if st.button("🔗 Reconciliation", width="stretch",
                     type="primary" if is_active else "secondary",
                     key="nav_recon"):
            st.session_state.active_section = "Reconciliation"
            st.rerun()

        st.divider()
        is_active = st.session_state.active_section == "All"
        if st.button("🗂️ Show All", width="stretch",
                     type="primary" if is_active else "secondary",
                     key="nav_all"):
            st.session_state.active_section = "All"
            st.rerun()

    # Clear All
    if has_any:
        st.divider()
        if st.button("🗑️ Clear All", width="stretch", type="secondary"):
            for k in ["debtor_dfs", "suspense_dfs", "receipt_dfs",
                      "uploaded_files_log", "recon_results"]:
                st.session_state[k] = {} if k != "uploaded_files_log" else []
            st.session_state.recon_ran      = False
            st.session_state.active_section = None
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────────────
title_col, logs_col = st.columns([3, 1])

with title_col:
    st.title("📊 Account Tracker")

with logs_col:
    count = len(st.session_state.uploaded_files_log)
    label = f"🗒️ Logs ({count})" if count > 0 else "🗒️ Logs"
    with st.expander(label, expanded=False):
        if st.session_state.uploaded_files_log:
            rows = ""
            for entry in reversed(st.session_state.uploaded_files_log):
                icon = {"Debtors": "📄", "Suspense": "🔄", "Receipt": "🧾"}.get(entry["type"], "📁")
                vertical_part = (
                    f" → <span style='color:#4CAF50;font-weight:600;'>{entry['vertical']}</span>"
                    if entry.get("vertical") else ""
                )
                rows += f"""
                <div style='padding:6px 0; border-bottom:1px solid #e8e8e8;
                            font-size:12px; line-height:1.6;'>
                    <div style='color:#999; font-size:11px;'>{entry['timestamp']}</div>
                    <div>{icon} <strong>{entry['type']}</strong>{vertical_part}</div>
                    <div style='color:#bbb; font-size:11px;
                                word-break:break-all;'>{entry['saved_name']}</div>
                </div>"""
            st.markdown(
                f"<div style='max-height:320px; overflow-y:auto;'>{rows}</div>",
                unsafe_allow_html=True
            )
        else:
            st.caption("No activity yet.")

# ─────────────────────────────────────────────────────────────────────────────
# Upload Panel
# ─────────────────────────────────────────────────────────────────────────────
if st.session_state.show_upload_panel:
    with st.container(border=True):
        st.subheader("📤 Upload Files")

        file_type = st.selectbox(
            "Select upload category",
            ["-- Select --", "Debtors", "Suspense", "Receipt"]
        )

        files_list = []
        verticals  = {}

        if file_type != "-- Select --":
            raw = st.file_uploader(
                f"Select {file_type} files",
                accept_multiple_files=True,
                type=["csv", "xlsx", "xls", "pdf", "txt"]
            )
            if raw is not None:
                files_list = raw if isinstance(raw, list) else [raw]

            if files_list:
                st.markdown("**Auto-detected verticals:**")
                for f in files_list:
                    v, source = detect_vertical(f, f.name)
                    verticals[f.name] = v
                    if v:
                        src_label = "📋 row 1" if source == "row1" else "📁 filename"
                        st.success(f"✅ `{f.name}` → **{v}** *(via {src_label})*")
                    else:
                        st.error(f"❌ `{f.name}` → Could not detect vertical")

        up_col1, up_col2, _ = st.columns([1, 1, 5])
        with up_col1:
            upload_btn = st.button("✅ Upload", type="primary")
        with up_col2:
            cancel_btn = st.button("✖ Cancel")

        if cancel_btn:
            st.session_state.show_upload_panel = False
            st.rerun()

        if upload_btn:
            if file_type == "-- Select --":
                st.warning("Please select a category.")
            elif not files_list:
                st.warning("Please select at least one file.")
            else:
                undetected = [f.name for f in files_list if not verticals.get(f.name)]
                if undetected:
                    st.error(
                        f"Cannot upload — vertical not detected for: "
                        f"{', '.join(undetected)}. Please rename and try again."
                    )
                else:
                    for f in files_list:
                        vertical   = verticals.get(f.name)
                        saved_name = generate_file_name(file_type, f.name, vertical)

                        st.session_state.uploaded_files_log.append({
                            "type":          file_type,
                            "original_name": f.name,
                            "saved_name":    saved_name,
                            "vertical":      vertical,
                            "timestamp":     __import__("datetime").datetime.now()
                                             .strftime("%Y-%m-%d %H:%M:%S"),
                            "size_kb":       round(f.size / 1024, 1),
                        })

                        try:
                            if file_type == "Debtors":
                                st.session_state.debtor_dfs[saved_name] = \
                                    parse_debtor_file(f, vertical)
                            elif file_type == "Suspense":
                                st.session_state.suspense_dfs[saved_name] = \
                                    parse_suspense_file(f, vertical)
                            elif file_type == "Receipt":
                                st.session_state.receipt_dfs[saved_name] = \
                                    parse_receipt_file(f, vertical)
                        except Exception as e:
                            st.warning(f"Could not parse {f.name}: {e}")

                    # Reset any prior recon results when new files are uploaded
                    st.session_state.recon_results = None
                    st.session_state.recon_ran     = False

                    st.success(f"✅ {len(files_list)} file(s) uploaded successfully!")
                    st.session_state.show_upload_panel = False
                    st.session_state.active_section    = file_type
                    st.rerun()

    st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Uploaded Files Table
# ─────────────────────────────────────────────────────────────────────────────
if st.session_state.uploaded_files_log:
    st.subheader("📂 Uploaded Files")
    df_log = pd.DataFrame(st.session_state.uploaded_files_log)
    df_log.columns = ["Category", "Original File", "Saved As",
                      "Vertical", "Uploaded At", "Size (KB)"]
    st.dataframe(df_log, width="stretch", hide_index=True)
else:
    st.info("No files uploaded yet. Click **⬆️ Upload Files** in the sidebar to begin.")

# ─────────────────────────────────────────────────────────────────────────────
# Debtors Analysis
# ─────────────────────────────────────────────────────────────────────────────
parsed_debtors = {k: v for k, v in st.session_state.debtor_dfs.items()
                  if v is not None and not v.empty}

if parsed_debtors and st.session_state.active_section in ("Debtors", "All"):
    st.divider()
    hdr_col, btn_col = st.columns([6, 1])
    with hdr_col:
        st.subheader("📊 Debtors Analysis")
    with btn_col:
        if st.button("🗑️ Clear", key="clear_debtors", type="secondary"):
            st.session_state.debtor_dfs = {}
            st.session_state.uploaded_files_log = [
                e for e in st.session_state.uploaded_files_log if e["type"] != "Debtors"
            ]
            st.session_state.recon_results = None
            st.session_state.recon_ran     = False
            if st.session_state.active_section == "Debtors":
                st.session_state.active_section = None
            st.rerun()

    combined = pd.concat(parsed_debtors.values(), ignore_index=True)
    total_all = combined.shape[0]
    combined["Month"] = pd.to_datetime(
        combined["Date"], format="%d-%b-%Y", errors="coerce"
    ).dt.strftime("%b /%y").str.lower()
    combined["Display Vertical"] = combined.apply(_resolve_display_vertical, axis=1)

    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    with col_f1:
        sel_vertical = st.multiselect(
            "Filter by Vertical",
            sorted(combined["Display Vertical"].unique().tolist()),
            key="d_vertical", placeholder="All"
        )
    with col_f2:
        sel_month = st.multiselect(
            "Filter by Month",
            sorted(combined["Month"].dropna().unique().tolist(),
                   key=lambda m: pd.to_datetime(m, format="%b /%y", errors="coerce")),
            key="d_month", placeholder="All"
        )
    with col_f3:
        sel_state = st.multiselect(
            "Filter by State",
            sorted(combined["State"].dropna().unique().tolist()),
            key="d_state", placeholder="All"
        )
    with col_f4:
        today = date.today()
        d_col1, d_col2 = st.columns(2)
        with d_col1:
            date_from = st.date_input("Date From",
                                      value=today - timedelta(days=730),
                                      key="d_date_from")
        with d_col2:
            date_to = st.date_input("Date To", value=today, key="d_date_to")

    col_f5, col_f6 = st.columns(2)
    with col_f5:
        sel_days_bucket = st.multiselect(
            "Filter by Days Bucket",
            sorted(combined["Days Bucket"].dropna().unique().tolist()),
            key="d_days_bucket", placeholder="All"
        )
    with col_f6:
        sel_amt_bucket = st.multiselect(
            "Filter by Amount Bucket",
            sorted(combined["Amount Bucket"].dropna().unique().tolist()),
            key="d_amt_bucket", placeholder="All"
        )

    filtered = combined.copy()
    if sel_vertical:    filtered = filtered[filtered["Display Vertical"].isin(sel_vertical)]
    if sel_month:       filtered = filtered[filtered["Month"].isin(sel_month)]
    if sel_state:       filtered = filtered[filtered["State"].isin(sel_state)]
    if sel_days_bucket: filtered = filtered[filtered["Days Bucket"].isin(sel_days_bucket)]
    if sel_amt_bucket:  filtered = filtered[filtered["Amount Bucket"].isin(sel_amt_bucket)]

    filtered["_date"] = pd.to_datetime(
        filtered["Date"], format="%d-%b-%Y", errors="coerce"
    ).dt.date
    filtered = filtered[
        (filtered["_date"] >= date_from) & (filtered["_date"] <= date_to)
    ].drop(columns=["_date"])

    met_d1, met_d2, met_d3 = st.columns(3)
    with met_d1:
        records_diff = total_all - len(filtered)
        st.metric("Records Displayed", f"{len(filtered):,}",
                  delta=f"-{records_diff:,} filtered out" if records_diff else "No filters applied",
                  delta_color="normal")
    with met_d2:
        st.metric("Total Amount", _fmt_inr(combined["Pending Amount"].sum()))
    with met_d3:
        amount_diff = combined["Pending Amount"].sum() - filtered["Pending Amount"].sum()
        st.metric("Pending Amount Displayed", _fmt_inr(filtered["Pending Amount"].sum()),
                  delta=f"-{_fmt_inr(amount_diff)} filtered out" if amount_diff else "No filters applied",
                  delta_color="normal")

    st.divider()
    st.dataframe(
        filtered[["Display Vertical", "Date", "Month", "Ref. No.",
                  "Party's Name", "State", "Pending Amount",
                  "Amount Bucket", "Days Overdue", "Days Bucket"]]
        .rename(columns={"Display Vertical": "Vertical"}),
        width="stretch", hide_index=True
    )

    # Download debtors
    dl_bytes = _to_excel({"Debtors": filtered.rename(columns={"Display Vertical": "Vertical"})})
    st.download_button("⬇️ Download Debtors", dl_bytes,
                       file_name="debtors_export.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="dl_debtors")

# ─────────────────────────────────────────────────────────────────────────────
# Suspense Analysis
# ─────────────────────────────────────────────────────────────────────────────
parsed_suspense = {k: v for k, v in st.session_state.suspense_dfs.items()
                   if v is not None and not v.empty}

if parsed_suspense and st.session_state.active_section in ("Suspense", "All"):
    st.divider()
    hdr_col, btn_col = st.columns([6, 1])
    with hdr_col:
        st.subheader("🔄 Suspense Analysis")
    with btn_col:
        if st.button("🗑️ Clear", key="clear_suspense", type="secondary"):
            st.session_state.suspense_dfs = {}
            st.session_state.uploaded_files_log = [
                e for e in st.session_state.uploaded_files_log if e["type"] != "Suspense"
            ]
            st.session_state.recon_results = None
            st.session_state.recon_ran     = False
            if st.session_state.active_section == "Suspense":
                st.session_state.active_section = None
            st.rerun()

    combined_s = pd.concat(parsed_suspense.values(), ignore_index=True)
    total_all_s = combined_s.shape[0]
    combined_s["Month"] = pd.to_datetime(
        combined_s["Date"], format="%d-%b-%Y", errors="coerce"
    ).dt.strftime("%b /%y").str.lower()

    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        sel_s_vertical = st.multiselect(
            "Filter by Vertical",
            sorted(combined_s["Vertical"].unique().tolist()),
            key="s_vertical", placeholder="All"
        )
    with col_s2:
        sel_s_month = st.multiselect(
            "Filter by Month",
            sorted(combined_s["Month"].dropna().unique().tolist(),
                   key=lambda m: pd.to_datetime(m, format="%b /%y", errors="coerce")),
            key="s_month", placeholder="All"
        )
    with col_s3:
        sel_s_vch = st.multiselect(
            "Filter by Vch Type",
            sorted(combined_s["Vch Type"].dropna().unique().tolist()),
            key="s_vch", placeholder="All"
        )

    filtered_s = combined_s.copy()
    if sel_s_vertical: filtered_s = filtered_s[filtered_s["Vertical"].isin(sel_s_vertical)]
    if sel_s_month:    filtered_s = filtered_s[filtered_s["Month"].isin(sel_s_month)]
    if sel_s_vch:      filtered_s = filtered_s[filtered_s["Vch Type"].isin(sel_s_vch)]

    met_s1, met_s2, met_s3 = st.columns(3)
    with met_s1:
        records_diff_s = total_all_s - len(filtered_s)
        st.metric("Records Displayed", f"{len(filtered_s):,}",
                  delta=f"-{records_diff_s:,} filtered out" if records_diff_s else "No filters applied",
                  delta_color="normal")
    with met_s2:
        st.metric("Total Credit", _fmt_inr(combined_s["Credit"].sum()))
    with met_s3:
        amount_diff_s = combined_s["Credit"].sum() - filtered_s["Credit"].sum()
        st.metric("Credit Displayed", _fmt_inr(filtered_s["Credit"].sum()),
                  delta=f"-{_fmt_inr(amount_diff_s)} filtered out" if amount_diff_s else "No filters applied",
                  delta_color="normal")

    st.divider()
    st.dataframe(
        filtered_s[["Vertical", "Date", "Month", "Particulars",
                    "Narration", "Vch Type", "Debit", "Credit"]],
        width="stretch", hide_index=True
    )

    dl_bytes_s = _to_excel({"Suspense": filtered_s})
    st.download_button("⬇️ Download Suspense", dl_bytes_s,
                       file_name="suspense_export.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="dl_suspense")

# ─────────────────────────────────────────────────────────────────────────────
# Receipt Analysis
# ─────────────────────────────────────────────────────────────────────────────
parsed_receipts = {k: v for k, v in st.session_state.receipt_dfs.items()
                   if v is not None and not v.empty}

if parsed_receipts and st.session_state.active_section in ("Receipt", "All"):
    st.divider()
    hdr_col, btn_col = st.columns([6, 1])
    with hdr_col:
        st.subheader("🧾 Receipt Analysis")
    with btn_col:
        if st.button("🗑️ Clear", key="clear_receipt", type="secondary"):
            st.session_state.receipt_dfs = {}
            st.session_state.uploaded_files_log = [
                e for e in st.session_state.uploaded_files_log if e["type"] != "Receipt"
            ]
            st.session_state.recon_results = None
            st.session_state.recon_ran     = False
            if st.session_state.active_section == "Receipt":
                st.session_state.active_section = None
            st.rerun()

    combined_r = pd.concat(parsed_receipts.values(), ignore_index=True)
    total_all_r = combined_r.shape[0]
    combined_r["Month"] = pd.to_datetime(
        combined_r["Date"], format="%d-%b-%Y", errors="coerce"
    ).dt.strftime("%b /%y").str.lower()

    # Tag direct vs suspense-routed for display
    combined_r["Type"] = combined_r["Particulars"].apply(
        lambda x: "Suspense-Routed" if str(x).strip().upper() == "SUSPENSE" else "Direct"
    )

    col_r1, col_r2, col_r3, col_r4 = st.columns(4)
    with col_r1:
        sel_r_vertical = st.multiselect(
            "Filter by Vertical",
            sorted(combined_r["Vertical"].unique().tolist()),
            key="r_vertical", placeholder="All"
        )
    with col_r2:
        sel_r_month = st.multiselect(
            "Filter by Month",
            sorted(combined_r["Month"].dropna().unique().tolist(),
                   key=lambda m: pd.to_datetime(m, format="%b /%y", errors="coerce")),
            key="r_month", placeholder="All"
        )
    with col_r3:
        sel_r_sheet = st.multiselect(
            "Filter by Sheet (Bank Account)",
            sorted(combined_r["Sheet"].dropna().unique().tolist()),
            key="r_sheet", placeholder="All"
        )
    with col_r4:
        sel_r_type = st.multiselect(
            "Filter by Type",
            ["Direct", "Suspense-Routed"],
            key="r_type", placeholder="All"
        )

    filtered_r = combined_r.copy()
    if sel_r_vertical: filtered_r = filtered_r[filtered_r["Vertical"].isin(sel_r_vertical)]
    if sel_r_month:    filtered_r = filtered_r[filtered_r["Month"].isin(sel_r_month)]
    if sel_r_sheet:    filtered_r = filtered_r[filtered_r["Sheet"].isin(sel_r_sheet)]
    if sel_r_type:     filtered_r = filtered_r[filtered_r["Type"].isin(sel_r_type)]

    met_r1, met_r2, met_r3, met_r4 = st.columns(4)
    with met_r1:
        records_diff_r = total_all_r - len(filtered_r)
        st.metric("Records Displayed", f"{len(filtered_r):,}",
                  delta=f"-{records_diff_r:,} filtered out" if records_diff_r else "No filters applied",
                  delta_color="normal")
    with met_r2:
        st.metric("Total Received", _fmt_inr(combined_r["Debit"].sum()))
    with met_r3:
        direct_total = combined_r[combined_r["Type"] == "Direct"]["Debit"].sum()
        st.metric("Direct Receipts", _fmt_inr(direct_total))
    with met_r4:
        susp_total = combined_r[combined_r["Type"] == "Suspense-Routed"]["Debit"].sum()
        st.metric("Routed to Suspense", _fmt_inr(susp_total))

    st.divider()
    st.dataframe(
        filtered_r[["Vertical", "Type", "Sheet", "Date", "Month",
                    "Vch Type", "Particulars", "Narration", "Debit", "Credit"]],
        width="stretch", hide_index=True
    )

    dl_bytes_r = _to_excel({"Receipts": filtered_r})
    st.download_button("⬇️ Download Receipts", dl_bytes_r,
                       file_name="receipts_export.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="dl_receipts")

# ─────────────────────────────────────────────────────────────────────────────
# Reconciliation Section
# ─────────────────────────────────────────────────────────────────────────────
if st.session_state.active_section == "Reconciliation":
    st.divider()
    st.subheader("🔗 Reconciliation")

    # ── Guard: check all three file types are present ─────────────────────────
    missing = []
    if not _has_df(st.session_state.debtor_dfs):   missing.append("Debtors")
    if not _has_df(st.session_state.suspense_dfs): missing.append("Suspense")
    if not _has_df(st.session_state.receipt_dfs):  missing.append("Receipts")

    if missing:
        st.warning(
            f"⚠️ Cannot run reconciliation. The following file type(s) have not been uploaded: "
            f"**{', '.join(missing)}**. Please upload them using the Upload Files button in the sidebar."
        )

    else:
        # ── Run button ────────────────────────────────────────────────────────
        run_col, info_col = st.columns([2, 5])
        with run_col:
            run_btn = st.button("▶️ Run Reconciliation", type="primary",
                                key="run_recon")
        with info_col:
            if st.session_state.recon_ran:
                st.success("✅ Reconciliation complete. Results shown below.")
            else:
                st.info("Click **Run Reconciliation** to process all uploaded files.")

        if run_btn:
            with st.spinner("Running reconciliation across all verticals..."):
                try:
                    results = run_all_verticals(
                        debtors_dfs  = {k: v for k, v in st.session_state.debtor_dfs.items()
                                        if v is not None and not v.empty},
                        receipts_dfs = {k: v for k, v in st.session_state.receipt_dfs.items()
                                        if v is not None and not v.empty},
                        suspense_dfs = {k: v for k, v in st.session_state.suspense_dfs.items()
                                        if v is not None and not v.empty},
                    )
                    st.session_state.recon_results = results
                    st.session_state.recon_ran     = True
                    st.rerun()
                except Exception as e:
                    st.error(f"Reconciliation failed: {e}")
                    st.exception(e)

        # ── Results ───────────────────────────────────────────────────────────
        if st.session_state.recon_ran and st.session_state.recon_results:
            results  = st.session_state.recon_results
            combined = results.get("combined", {})
            summaries = results.get("summaries", [])

            # ── Summary metrics ───────────────────────────────────────────────
            if summaries:
                total_bills = sum(s["debtors_bills"] for s in summaries)
                total_cleared = sum(s["bills_cleared"] for s in summaries)
                total_partial = sum(s["bills_partial"] for s in summaries)
                total_open = sum(s["bills_open"] for s in summaries)
                total_amt_cl = sum(s["total_cleared"] for s in summaries)
                total_amt_out = sum(s["total_outstanding"] for s in summaries)
                total_susp_app = sum(s["suspense_applied"] for s in summaries)
                total_susp_rem = sum(s["suspense_remaining"] for s in summaries)
                total_kb_gaps = sum(s["kb_gaps"] for s in summaries)
                total_unresolv = sum(s["unresolvable"] for s in summaries)

                st.markdown("### Summary")
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Total Bills", f"{total_bills:,}")
                m2.metric("Cleared", f"{total_cleared:,}")
                m3.metric("Partial", f"{total_partial:,}")
                m4.metric("Open", f"{total_open:,}")
                m5.metric("KB Gaps", f"{total_kb_gaps:,}")

                m6, m7, m8, m9, m10 = st.columns(5)
                m6.metric("Amount Cleared", _fmt_inr(total_amt_cl))
                m7.metric("Outstanding", _fmt_inr(total_amt_out))
                m8.metric("Suspense Applied", _fmt_inr(total_susp_app))
                m9.metric("Suspense Remaining", _fmt_inr(total_susp_rem))
                m10.metric("Unresolvable", f"{total_unresolv:,}")

                # Per-vertical breakdown
                with st.expander("📋 Per-Vertical Breakdown", expanded=False):
                    summary_df = pd.DataFrame(summaries)
                    st.dataframe(summary_df, width="stretch", hide_index=True)

            st.divider()

            # ── Result Tabs ───────────────────────────────────────────────────
            tab_stmt, tab_debtors, tab_suspense, tab_gaps, tab_unresolv = st.tabs([
                "📋 Statement",
                "📊 Updated Debtors",
                "🔄 Updated Suspense",
                "🔍 KB Gaps",
                "⚠️ Unresolvable",
            ])

            # ── Tab 1: Reconciliation Statement ──────────────────────────────
            with tab_stmt:
                df_stmt = combined.get("statement", pd.DataFrame())
                if df_stmt is not None and not df_stmt.empty:
                    st.markdown(f"**{len(df_stmt):,} knock-off events across all verticals**")

                    if "Vertical" in df_stmt.columns:
                        verts_stmt = sorted(df_stmt["Vertical"].unique().tolist())
                        sel_stmt_v = st.multiselect("Filter by Vertical", verts_stmt,
                                                    key="stmt_v", placeholder="All")
                        if sel_stmt_v:
                            df_stmt = df_stmt[df_stmt["Vertical"].isin(sel_stmt_v)]

                    # Capture raw confidence before translation for colour-coding
                    raw_conf_list = (
                        df_stmt["Match_Confidence"].tolist()
                        if "Match_Confidence" in df_stmt.columns else []
                    )
                    display_stmt = apply_statement_presentation(df_stmt, for_excel=False)

                    if "Confidence" in display_stmt.columns and raw_conf_list:
                        conf_idx_st = display_stmt.columns.get_loc("Confidence")
                        def _colour_conf_row(row):
                            styles = [""] * len(row)
                            try:
                                styles[conf_idx_st] = _conf_colour(
                                    raw_conf_list[row.name] if row.name < len(raw_conf_list) else ""
                                )
                            except Exception:
                                pass
                            return styles
                        styled_stmt = display_stmt.reset_index(drop=True).style.apply(
                            _colour_conf_row, axis=1
                        )
                    else:
                        styled_stmt = display_stmt

                    st.dataframe(styled_stmt, width="stretch", hide_index=True)
                else:
                    st.info("No knock-off events recorded.")

            # ── Tab 2: Updated Debtors ────────────────────────────────────────
            with tab_debtors:
                df_ud = combined.get("updated_debtors", pd.DataFrame())
                if df_ud is not None and not df_ud.empty:
                    fc1, fc2, fc3 = st.columns(3)
                    with fc1:
                        # Filter uses internal Status values before translation
                        status_opts = sorted(df_ud["Status"].unique().tolist()) \
                                      if "Status" in df_ud.columns else []
                        sel_ud_status = st.multiselect("Filter by Status", status_opts,
                                                       key="ud_status", placeholder="All")
                    with fc2:
                        vert_opts = sorted(df_ud["Vertical"].unique().tolist()) \
                                    if "Vertical" in df_ud.columns else []
                        sel_ud_v = st.multiselect("Filter by Vertical", vert_opts,
                                                  key="ud_v", placeholder="All")
                    with fc3:
                        party_opts = sorted(df_ud["Party's Name"].dropna().unique().tolist()) \
                                     if "Party's Name" in df_ud.columns else []
                        sel_ud_party = st.multiselect("Filter by Client", party_opts,
                                                      key="ud_party", placeholder="All")

                    # Filter on raw df (internal names), then apply presentation
                    filt_ud = df_ud.copy()
                    if sel_ud_status: filt_ud = filt_ud[filt_ud["Status"].isin(sel_ud_status)]
                    if sel_ud_v:      filt_ud = filt_ud[filt_ud["Vertical"].isin(sel_ud_v)]
                    if sel_ud_party:  filt_ud = filt_ud[filt_ud["Party's Name"].isin(sel_ud_party)]

                    um1, um2, um3 = st.columns(3)
                    um1.metric("Bills Shown", f"{len(filt_ud):,}")
                    if "Remaining_Amount" in filt_ud.columns:
                        um2.metric("Remaining Outstanding",
                                   _fmt_inr(pd.to_numeric(filt_ud["Remaining_Amount"],
                                                           errors="coerce").sum()))
                    if "Cleared_Amount" in filt_ud.columns:
                        um3.metric("Cleared Amount",
                                   _fmt_inr(pd.to_numeric(filt_ud["Cleared_Amount"],
                                                           errors="coerce").sum()))

                    raw_status_ud = filt_ud["Status"].tolist() if "Status" in filt_ud.columns else []
                    display_ud = apply_debtors_presentation(filt_ud, for_excel=False)

                    if "Status" in display_ud.columns and raw_status_ud:
                        status_idx_ud = display_ud.columns.get_loc("Status")
                        def _colour_status_row(row):
                            styles = [""] * len(row)
                            try:
                                styles[status_idx_ud] = _status_colour(
                                    raw_status_ud[row.name] if row.name < len(raw_status_ud) else ""
                                )
                            except Exception:
                                pass
                            return styles
                        styled_ud = display_ud.reset_index(drop=True).style.apply(
                            _colour_status_row, axis=1
                        )
                    else:
                        styled_ud = display_ud

                    st.dataframe(styled_ud, width="stretch", hide_index=True)
                else:
                    st.info("No updated debtors data.")

            # ── Tab 3: Updated Suspense ───────────────────────────────────────
            with tab_suspense:
                df_us = combined.get("updated_suspense", pd.DataFrame())
                if df_us is not None and not df_us.empty:
                    fc1, fc2 = st.columns(2)
                    with fc1:
                        status_opts_s = sorted(df_us["Status"].unique().tolist()) \
                                        if "Status" in df_us.columns else []
                        sel_us_status = st.multiselect("Filter by Status", status_opts_s,
                                                       key="us_status", placeholder="All")
                    with fc2:
                        vert_opts_s = sorted(df_us["Vertical"].unique().tolist()) \
                                      if "Vertical" in df_us.columns else []
                        sel_us_v = st.multiselect("Filter by Vertical", vert_opts_s,
                                                  key="us_v", placeholder="All")

                    filt_us = df_us.copy()
                    if sel_us_status: filt_us = filt_us[filt_us["Status"].isin(sel_us_status)]
                    if sel_us_v:      filt_us = filt_us[filt_us["Vertical"].isin(sel_us_v)]

                    sm1, sm2, sm3 = st.columns(3)
                    sm1.metric("Entries Shown", f"{len(filt_us):,}")
                    if "Applied_Amount" in filt_us.columns:
                        sm2.metric("Applied to Invoice",
                                   _fmt_inr(pd.to_numeric(filt_us["Applied_Amount"],
                                                           errors="coerce").sum()))
                    if "Remaining_Amount" in filt_us.columns:
                        sm3.metric("Carry Forward",
                                   _fmt_inr(pd.to_numeric(filt_us["Remaining_Amount"],
                                                           errors="coerce").sum()))

                    raw_status_us = filt_us["Status"].tolist() if "Status" in filt_us.columns else []
                    display_us = apply_suspense_presentation(filt_us, for_excel=False)

                    if "Status" in display_us.columns and raw_status_us:
                        status_idx_us = display_us.columns.get_loc("Status")
                        def _colour_susp_row(row):
                            styles = [""] * len(row)
                            try:
                                styles[status_idx_us] = _status_colour(
                                    raw_status_us[row.name] if row.name < len(raw_status_us) else ""
                                )
                            except Exception:
                                pass
                            return styles
                        styled_us = display_us.reset_index(drop=True).style.apply(
                            _colour_susp_row, axis=1
                        )
                    else:
                        styled_us = display_us

                    st.dataframe(styled_us, width="stretch", hide_index=True)
                else:
                    st.info("No updated suspense data.")

            # ── Tab 4: KB Gaps ────────────────────────────────────────────────
            with tab_gaps:
                df_gaps = combined.get("kb_gaps", pd.DataFrame())
                df_debtors_for_gaps = combined.get("updated_debtors", pd.DataFrame())
                if df_gaps is not None and not df_gaps.empty:
                    st.markdown(
                        f"**{len(df_gaps):,} entries could not be fully matched.** "
                        f"Download the report, fill in **Confirmed Client**, and update the KB / PNM."
                    )

                    fc_g1, fc_g2 = st.columns(2)
                    with fc_g1:
                        sel_g_v = st.multiselect(
                            "Filter by Vertical",
                            sorted(df_gaps["Vertical"].unique().tolist()) if "Vertical" in df_gaps.columns else [],
                            key="g_v", placeholder="All"
                        )
                    with fc_g2:
                        sel_g_reason = st.multiselect(
                            "Filter by Gap Reason",
                            # Show translated values in the filter
                            sorted(set(
                                apply_kb_gaps_presentation(
                                    df_gaps, df_debtors_for_gaps, for_excel=False
                                )["Gap Reason"].dropna().unique().tolist()
                            )) if not df_gaps.empty else [],
                            key="g_reason", placeholder="All"
                        )

                    # Filter on raw df first, then apply presentation
                    filt_g_raw = df_gaps.copy()
                    if sel_g_v: filt_g_raw = filt_g_raw[filt_g_raw["Vertical"].isin(sel_g_v)]

                    display_g = apply_kb_gaps_presentation(
                        filt_g_raw, df_debtors_for_gaps, for_excel=False
                    )
                    if sel_g_reason and "Gap Reason" in display_g.columns:
                        display_g = display_g[display_g["Gap Reason"].isin(sel_g_reason)]

                    st.dataframe(display_g, width="stretch", hide_index=True)
                else:
                    st.success("✅ No KB gaps — all entries were resolved.")

            # ── Tab 5: Unresolvable ───────────────────────────────────────────
            with tab_unresolv:
                df_ur = combined.get("unresolvable", pd.DataFrame())
                if df_ur is not None and not df_ur.empty:
                    st.markdown(
                        f"**{len(df_ur):,} entries could not be resolved** — "
                        f"no client candidate found. These require manual review."
                    )

                    fc_u1, fc_u2 = st.columns(2)
                    with fc_u1:
                        sel_u_v = st.multiselect(
                            "Filter by Vertical",
                            sorted(df_ur["Vertical"].unique().tolist()) if "Vertical" in df_ur.columns else [],
                            key="u_v", placeholder="All"
                        )
                    with fc_u2:
                        sel_u_reason = st.multiselect(
                            "Filter by Why Unresolved",
                            # Show translated reason values in the filter
                            sorted(set(
                                apply_unresolvable_presentation(df_ur, for_excel=False
                                )["Why Unresolved"].dropna().unique().tolist()
                            )) if not df_ur.empty else [],
                            key="u_reason", placeholder="All"
                        )

                    filt_u_raw = df_ur.copy()
                    if sel_u_v: filt_u_raw = filt_u_raw[filt_u_raw["Vertical"].isin(sel_u_v)]

                    display_u = apply_unresolvable_presentation(filt_u_raw, for_excel=False)
                    if sel_u_reason and "Why Unresolved" in display_u.columns:
                        display_u = display_u[display_u["Why Unresolved"].isin(sel_u_reason)]

                    st.dataframe(display_u, width="stretch", hide_index=True)
                else:
                    st.success("✅ No unresolvable entries.")

            # ── Download All ──────────────────────────────────────────────────
            st.divider()
            dl_col, _ = st.columns([2, 5])
            with dl_col:
                excel_bytes = build_reconciliation_report(
                    combined  = combined,
                    summaries = summaries,
                )
                st.download_button(
                    "📥 Export Reconciliation Report",
                    excel_bytes,
                    file_name="Reconciliation_Report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_recon_styled",
                )