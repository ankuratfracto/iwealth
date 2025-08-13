# app.py


# Ensure the latest mapping.yaml changes are picked up
import importlib, mcc as _mcc_mod
importlib.reload(_mcc_mod)
from mcc import FORMATS     # refresh the constant after reload

import io, textwrap
import streamlit as st
import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import base64
from mcc import call_fracto_parallel, write_excel_from_ocr, stamp_job_number
from PyPDF2 import PdfReader

# ── Page config (must be first Streamlit command) ─────────────
st.set_page_config(
    page_title="PDF → Smart‑OCR → Excel",
    page_icon="📄",
    layout="wide",
)

# ── Fracto branding styles ────────────────────────────────────
FRACTO_PRIMARY   = "#00AB6B"   # adjust if brand palette differs
FRACTO_DARK      = "#00895A"
FRACTO_LIGHT_BG  = "#F5F8FF"

st.markdown(f"""
    <style>
    /* Page background */
    .stApp {{
        background: {FRACTO_LIGHT_BG};
    }}
    /* Center main content max-width 880px */
    .main .block-container{{
        max-width:880px;
        margin:auto;
    }}
    .block-container{{
        max-width:880px !important;
        margin-left:auto !important;
        margin-right:auto !important;
    }}
    /* Primary buttons */
    button[kind="primary"] {{
        background-color: {FRACTO_PRIMARY} !important;
        color: #fff !important;
        border: 0 !important;
    }}
    button[kind="primary"]:hover {{
        background-color: {FRACTO_DARK} !important;
        color: #fff !important;
    }}
    /* Header text color */
    h1 {{
        color: {FRACTO_DARK};
    }}
    /* Manual text_input boxes: white background & border */
    .stTextInput > div > div > input {{
        background-color: #ffffff !important;
        border: 1px solid #CCCCCC !important;
        border-radius: 4px !important;
    }}
    .stTextInput > div > div > input:focus {{
        border: 1px solid #00AB6B !important;   /* Fracto primary on focus */
        box-shadow: 0 0 0 2px rgba(0,171,107,0.2) !important;
    }}
    /* File uploader box */
    .stFileUploader > div > div {{
        background-color: #ffffff !important;
        border: 1px solid #CCCCCC !important;
        border-radius: 4px !important;
        color: #222222 !important;
    }}
    /* Fix inside text in uploader */
    .stFileUploader label {{
        color: #222222 !important;
    }}
    /* Force background and text for all blocks */
    html, body, .stApp, .block-container {{
        background-color: #FFFFFF !important;
        color: #222222 !important;
    }}
    /* Buttons in login section */
    button, .stButton button {{
        background-color: #00AB6B !important;
        color: #ffffff !important;
    }}
    button:hover, .stButton button:hover {{
        background-color: #00895A !important;
        color: #ffffff !important;
    }}
    /* Labels stay dark text */
    label, .stMarkdown, .stSubheader, .stHeader, .stTextInput label {{
        color: #222222 !important;
    }}
    /* Password input */
    input[type="password"] {{
        background-color: #FFFFFF !important;
        color: #222222 !important;
        border: 1px solid #CCCCCC !important;
    }}
    /* Duplicate overrides in dark‑mode query */
    @media (prefers-color-scheme: dark) {{
        html, body, .stApp, .block-container {{
            background-color: #FFFFFF !important;
            color: #222222 !important;
        }}
        label, .stMarkdown, .stSubheader, .stHeader, .stTextInput label {{
            color: #222222 !important;
        }}
        input[type="password"] {{
            background-color: #FFFFFF !important;
            color: #222222 !important;
            border: 1px solid #CCCCCC !important;
        }}
    }}
    /* Force light theme when user is in dark mode */
    @media (prefers-color-scheme: dark) {{
        .stApp {{
            background: #FFFFFF !important;
        }}
        h1, h2, h3, h4, h5, h6, p, label, span, div, input, textarea {{
            color: #222222 !important;
        }}
        /* keep our primary buttons */
        button[kind="primary"] {{
            background-color: #00AB6B !important;
            color: #fff !important;
        }}
        button[kind="primary"]:hover {{
            background-color: #00895A !important;
        }}
        /* inputs */
        .stTextInput > div > div > input {{
            background-color: #ffffff !important;
            color: #222222 !important;
        }}
        /* uploader stays light */
        .stFileUploader > div > div {{
            background-color: #ffffff !important;
            border: 1px solid #CCCCCC !important;
            color: #222222 !important;
        }}
        .stFileUploader label {{
            color: #222222 !important;
        }}
    }}
    /* Force hover shadow on cards */
    .card:hover {{
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        transition: box-shadow 0.3s ease-in-out;
    }}
    /* Scrolling logo strip */
    .logo-strip-wrapper{{
        max-width:880px;
        margin:24px auto;
        overflow:hidden;
    }}
    .logo-strip{{
        display:inline-block;
        white-space:nowrap;
        animation:logoscroll 20s linear infinite; /* doubled speed */
    }}
    .logo-strip img{{
        height:48px;
        margin:0 32px;
        vertical-align:middle;
        display:inline-block;
    }}
    /* Remove extra gap where the duplicated sequence joins */
    .logo-strip img:last-child{{
        margin-right:0;
    }}
    /* Remove margin-left on first clone to shorten overall gap */
    .logo-strip img:nth-child(1){{
        margin-left:0;
    }}
    @keyframes logoscroll{{
        0%   {{transform:translateX(0);}}
        100% {{transform:translateX(-50%);}}
    }}
    </style>
""", unsafe_allow_html=True)
# ── Clients logo strip ───────────────────────────────────────
def build_logo_strip(logo_paths: list[str]) -> str:
    """
    Return HTML for the scrolling logo strip.
    Each file is read from disk and embedded as a Base64 data‑URI,
    so it renders correctly on Streamlit Cloud.
    """
    tags = ""
    script_dir = Path(__file__).parent
    for rel_path in logo_paths:
        img_path = (script_dir / rel_path).expanduser().resolve()
        if not img_path.exists():
            continue
        mime = "image/svg+xml" if img_path.suffix.lower() == ".svg" else "image/png"
        b64   = base64.b64encode(img_path.read_bytes()).decode("utf-8")
        tags += f"<img src='data:{mime};base64,{b64}' alt='' />"
    # Duplicate sequence so the CSS animation loops seamlessly
    return f"<div class='logo-strip-wrapper'><div class='logo-strip'>{tags}{tags}</div></div>"

st.markdown(
    """
    <style>
    .card-container {
        display: flex;
        gap: 1rem;
        flex-wrap: wrap;
        margin-bottom: 1rem;
    }
    .card {
        flex: 1 1 200px;
        background: #F6F8FA;
        border: 1px solid #E0E0E0;
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        padding: 1rem;
        text-align: center;
    }
    .card-icon {
        margin-bottom: 8px;
        display:flex;
        justify-content:center;
    }
    .card-icon img{
        width:36px;
        height:36px;
    }
    .card h4{
        font-size:16px;
        font-weight:600;
        margin:4px 0 8px 0;
        color: #00895A;
    }
    .card p{
        font-size:13px;
        line-height:1.4rem;
        margin:0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Logo banner at the top
st.image("fractologo.jpeg", width=180)

# ── Session keys ─────────────────────────────────────────────
if "excel_bytes" not in st.session_state:
    st.session_state["excel_bytes"] = None
if "excel_filename" not in st.session_state:
    st.session_state["excel_filename"] = ""
if "edited_excel_bytes" not in st.session_state:
    st.session_state["edited_excel_bytes"] = None
    st.session_state["edited_filename"] = ""

# ── Simple username/password gate ─────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    st.subheader("🔐 Login required")
    uname = st.text_input("Username")
    pword = st.text_input("Password", type="password")
    if st.button("Login"):
        if uname == "mcc" and pword == "mcc@99":
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Invalid credentials")
    st.stop()   # prevent the rest of the app from rendering

# Ensure FRACTO_API_KEY is available for mcc.call_fracto
if "FRACTO_API_KEY" in st.secrets:
    os.environ["FRACTO_API_KEY"] = st.secrets["FRACTO_API_KEY"]



# ── Hero / intro ──────────────────────────────────────────────
st.markdown(
    '''
    <h2 style="color:#00895A;font-weight:600;margin-bottom:0.2rem;">Automate imports. Eliminate re‑typing.</h2>
    <p style="font-size:1.05rem;line-height:1.5rem;margin-bottom:1.5rem;">
      Fracto converts your shipping invoices, customs docs and purchase orders into<br>
      ERP‑ready spreadsheets in seconds — complete with your business rules and validation checks.
    </p>
    ''',
    unsafe_allow_html=True,
)
# 24px spacing before uploader
st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

st.markdown("## Smart‑OCR to ERP‑ready Excel")

st.markdown('<h3 id="upload">1. Upload and process your PDF</h3>', unsafe_allow_html=True)

# ── Upload & Process ──────────────────────────────────────────
# Upload widget
pdf_file = st.file_uploader("Upload PDF", type=["pdf"])

# Show thumbnail info after upload
if pdf_file:
    # Show file thumbnail info
    file_size_kb = pdf_file.size / 1024
    try:
        page_count = len(PdfReader(pdf_file).pages)
    except Exception:
        page_count = "?"
    st.info(f"**{pdf_file.name}**  •  {file_size_kb:,.1f} KB  •  {page_count} page(s)")
    # Reset file pointer for later reading
    pdf_file.seek(0)

# ── Manual overrides ─────────────────────────────────────────────
st.markdown("#### Optional manual fields")
manual_inputs: dict[str, str] = {}
job_no: str | None = None

manual_fields = ["Job Number"]    # add more keys here if needed
TOOLTIPS = {
    "Job Number": "Stamped at the top of every page in the PDF.",
}
for col in manual_fields:
    val = st.text_input(col, key=f"manual_{col}", help=TOOLTIPS.get(col, ""))
    if not val:
        continue
    if col == "Job Number":
        job_no = val          # used only for PDF stamping
    else:
        manual_inputs[col] = val  # Excel overrides

# Formats come straight from mapping.yaml ("Format 1", "Format 2", …)
format_names = list(FORMATS.keys())
selected_format_key = st.selectbox("Select Excel output format", format_names)
selected_format_cfg = FORMATS[selected_format_key]

# Process button
run = st.button("⚙️ Process PDF", disabled=pdf_file is None)

if run:
    if not pdf_file:
        st.warning("Please upload a PDF first.")
        st.stop()

    progress = st.progress(0.0, text="Uploading & extracting …")
    try:
        pdf_bytes = pdf_file.read()
        progress.progress(0.2)
        if job_no:
            pdf_bytes = stamp_job_number(pdf_bytes, job_no)
        progress.progress(0.4)

        results = call_fracto_parallel(pdf_bytes, pdf_file.name)
        progress.progress(0.8)

        buffer = io.BytesIO()
        write_excel_from_ocr(
            results,
            buffer,
            overrides=manual_inputs,
            mappings=selected_format_cfg["mappings"],
            template_path=selected_format_cfg.get("template_path"),
            sheet_name=selected_format_cfg.get("sheet_name"),
        )
        progress.progress(1.0, text="Done!")
        st.session_state["excel_bytes"]   = buffer.getvalue()
        base_name = Path(pdf_file.name).stem
        st.session_state["excel_filename"] = f"{base_name}_ocr.xlsx"
        st.toast("✅ Excel generated!", icon="🎉")
    except Exception as exc:
        st.toast(f"❌ Error: {exc}", icon="⚠️")
        st.error(f"Processing failed: {exc}")
        st.stop()

# ── Preview & download ────────────────────────────────────────
if st.session_state["excel_bytes"]:
    st.markdown("### 2. Review and export")
    st.download_button(
        "⬇️ Download original Excel",
        data=st.session_state["excel_bytes"],
        file_name=st.session_state["excel_filename"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_original",
    )

    df = pd.read_excel(io.BytesIO(st.session_state["excel_bytes"]))
    edited_df = st.data_editor(
        df,
        num_rows="dynamic",
        use_container_width=True,
        key="editable_grid",
    )

    if st.button("💾 Save edits"):
        # Load original workbook to preserve formatting
        from openpyxl import load_workbook
        wb_orig = load_workbook(io.BytesIO(st.session_state["excel_bytes"]))
        ws      = wb_orig.active

        # Overwrite data rows (assumes header is row 1)
        for r_idx, (_, row) in enumerate(edited_df.iterrows(), start=2):
            for c_idx, value in enumerate(row, start=1):
                ws.cell(row=r_idx, column=c_idx, value=value)

        out_buf = io.BytesIO()
        wb_orig.save(out_buf)
        st.session_state["edited_excel_bytes"] = out_buf.getvalue()
        st.session_state["edited_filename"] = (
            Path(st.session_state["excel_filename"]).with_suffix("").name + "_edited.xlsx"
        )
        st.success("Edits saved — scroll below to download the .xlsx file.")

    if st.session_state.get("edited_excel_bytes"):
        st.download_button(
            "⬇️ Download edited Excel",
            data=st.session_state["edited_excel_bytes"],
            file_name=st.session_state["edited_filename"],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_edited",
        )

    # ── Quick stats & visualisations ───────────────────────────
    view_df = edited_df if st.session_state.get("edited_excel_bytes") else df

    st.markdown("### 3. Quick stats")
    k1, k2 = st.columns(2)
    k1.metric("Total rows", view_df.shape[0])
    k2.metric("Blank cells", int(view_df.isna().sum().sum()))

    # Optionally show numeric totals if columns exist
    if "Qty" in view_df.columns:
        st.metric("Total Qty", f"{view_df['Qty'].sum():,.0f}")
    if "Unit Price" in view_df.columns:
        total_unit_price = (
            pd.to_numeric(view_df["Unit Price"], errors="coerce")  # convert non‑numbers → NaN
              .fillna(0)                                           # treat NaNs as 0
              .sum()
        )
        st.metric("Sum Unit Price", f"{total_unit_price:,.0f}")

    # ── Top Part Numbers by Qty chart ─────────────────────────
    if {"Part No.", "Qty"}.issubset(view_df.columns):
        st.markdown("#### Top SKUs by Qty")
        top_qty = (
            view_df.groupby("Part No.")["Qty"]
            .sum(numeric_only=True)
            .sort_values(ascending=False)
            .head(10)
        )

        if top_qty.empty or top_qty.shape[0] < 1:
            st.info("No Qty data available to plot.")
        else:
            fig, ax = plt.subplots()
            top_qty.plot(kind="barh", ax=ax)
            ax.invert_yaxis()
            ax.set_xlabel("Qty")
            ax.set_ylabel("Part No.")
            st.pyplot(fig)

st.markdown("---")

# ── Clients logo strip ───────────────────────────────────────
st.markdown("### Our Clients")
logo_files = [
    "clients/kuhoo.png",
    "clients/ODeX.png",
    "clients/accomation.png",
    "clients/jaikisan.png",
    "clients/121Finance.png",
    "clients/NBHC.png",
    "clients/MCC.png",
    "clients/navata.png",
    "clients/trukker.png",
    "clients/turno.png",
    "clients/petpooja.png",
    "clients/freightfox.png",
    "clients/presolv.png",
    "clients/equal.png",
    "clients/ambit.png",
    "clients/khfl.png",
    "clients/pssc.png",
    "clients/symbo.png",
]
st.markdown(build_logo_strip(logo_files), unsafe_allow_html=True)
st.markdown("---")

# ── Benefits grid ─────────────────────────────────────────────
st.markdown("### Why choose **Fracto Imports**?")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("#### 🚀 10× Faster")
    st.write("Upload → processed Excel in under a minute, even for multi‑page PDFs.")
with col2:
    st.markdown("#### 🔍 Error‑free")
    st.write("AI‑assisted extraction + your manual overrides ensure 99.9 % accuracy.")
with col3:
    st.markdown("#### 🔗 Fits Your ERP")
    st.write("Column mapping matches your import template out‑of‑the‑box.")

st.markdown("---")


# ── Inline SVG icons (Tabler, 36×36, stroke‑currentColor) ─
ICONS = {
    "upload": '''
      <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" stroke="#00895A" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2" /><polyline points="7 9 12 4 17 9" /><line x1="12" y1="4" x2="12" y2="16" /></svg>
    ''',
    "cpu": '''
      <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" stroke="#00895A" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="1" /><rect x="9" y="9" width="6" height="6" rx="1" /><path d="M3 9h1" /><path d="M3 15h1" /><path d="M20 9h1" /><path d="M20 15h1" /><path d="M9 3v1" /><path d="M15 3v1" /><path d="M9 20v1" /><path d="M15 20v1" /></svg>
    ''',
    "edit": '''
      <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" stroke="#00895A" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 3l4 4l-11 11h-4v-4z" /><path d="M13 6l4 4" /><path d="M3 20v1h1l3-3" /></svg>
    ''',
    "export": '''
      <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" stroke="#00895A" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 3v4a1 1 0 0 0 1 1h4" /><path d="M5 12v-7a2 2 0 0 1 2 -2h7l5 5v4" /><path d="M9 15l3 -3l3 3" /><path d="M12 12v9" /></svg>
    ''',
    "ship": '''
      <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" stroke="#00895A" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9 -4l9 4l-9 4z" /><path d="M3 9l9 4l9 -4" /><path d="M12 19l0 -11" /><path d="M9 21l-1 -7" /><path d="M15 21l1 -7" /></svg>
    ''',
    "factory": '''
      <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" stroke="#00895A" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21v-13l8 -4v7l8 -4v14" /><path d="M13 13l-8 -4" /><path d="M5 17h2v4h-2z" /><path d="M9 17h2v4h-2z" /><path d="M13 17h2v4h-2z" /><path d="M17 17h2v4h-2z" /></svg>
    ''',
    "dollar": '''
      <svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 24 24" stroke="#00895A" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v18" /><path d="M17 8a5 5 0 0 0 -10 0c0 5 5 3 10 8a5 5 0 0 1 -10 0" /></svg>
    ''',
}

# ── Card rendering helper ─────────────────────────────────────
def render_card(icon_name: str, title: str, body: str, *, width="250px") -> str:
    svg = ICONS.get(icon_name, "")
    return f"""
        <div class="card" style="max-width:{width};">
          <div class="card-icon">{svg}</div>
          <h4>{title}</h4>
          <p>{body}</p>
        </div>
    """

# ── How it works ──────────────────────────────────────────────
st.markdown('<h3 id="how">How it works</h3>', unsafe_allow_html=True)

steps = [
    ("upload", "Upload", "Drag PDFs or images of invoices, POs, customs docs into the drop‑zone."),
    ("cpu", "AI Extraction", "Reads tables, handwriting and stamps with 99 %+ accuracy."),
    ("edit", "Review & Edit", "Adjust any field inline — spreadsheet‑style editor keeps you in control."),
    ("export", "Export", "Download ERP‑ready Excel or push straight into your system via API."),
]

cols = st.columns(4)
for col, (icon_name, title, body) in zip(cols, steps):
    with col:
        col.markdown(render_card(icon_name, title, body), unsafe_allow_html=True)

st.markdown("---")

# ── Popular use‑cases ─────────────────────────────────────────
st.markdown('<h3 id="usecases">Popular use‑cases</h3>', unsafe_allow_html=True)

use_cases = [
    ("ship", "Import Logistics", "Bills of lading, packing lists, HS‑code mapping — ready for customs clearance."),
    ("factory", "Manufacturing", "Supplier invoices and QC sheets flow directly into SAP/Oracle with serial‑level traceability."),
    ("dollar", "Finance & AP", "Reconcile bank statements and purchase invoices 10× faster with zero manual key‑in."),
]

uc_cols = st.columns(3)
for col, (icon_name, title, body) in zip(uc_cols, use_cases):
    with col:
        col.markdown(render_card(icon_name, title, body, width="280px"), unsafe_allow_html=True)

st.markdown("---")

# ── Footer ────────────────────────────────────────────────────
st.markdown(
    "<div style='text-align:center;font-size:0.85rem;padding-top:2rem;color:#666;'>"
    "Made with ❤️ by <a href='https://www.fracto.tech' style='color:#00AB6B;' target='_blank'>Fracto</a>"
    "</div>",
    unsafe_allow_html=True,
)