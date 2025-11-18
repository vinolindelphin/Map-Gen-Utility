# streamlit run app.py -FINAL
import re, json, calendar
from datetime import date
from dateutil.relativedelta import relativedelta

import os
import pandas as pd
import geopandas as gpd
import streamlit as st
import folium
from streamlit.components.v1 import html as st_html

from google.cloud import bigquery
from google.oauth2 import service_account

SHOW_DEBUG = True  # <- set True only when you want to see auth/status tiles


# ================= CONFIG =================
GEOJSON_PATH = "All_India_pincode_Boundary-19312.geojson"
SIMPLIFY_TOLERANCE_M = 500  # 0 disables

# Colors: dark red -> dark green
R2G8 = ["#8B0000","#B22222","#FF0000","#FF4500","#FF7F00",
        "#FFD700","#90EE90","#006400"]
import os, json
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# For Python 3.11+, tomllib is built-in. If you are on 3.10 use:  pip install tomli
try:
    import tomllib  # py311+
except Exception:
    import tomli as tomllib  # py310 fallback

def _load_sa_from_toml_files():
    """
    Try to read gcp_service_account from a secrets.toml file on disk:
      1) %USERPROFILE%\.streamlit\secrets.toml
      2) <CWD>\.streamlit\secrets.toml
    Returns (dict_or_None, source_str)
    """
    candidates = [
        os.path.join(os.environ.get("USERPROFILE", ""), ".streamlit", "secrets.toml"),
        os.path.join(os.getcwd(), ".streamlit", "secrets.toml"),
    ]
    for path in candidates:
        try:
            if path and os.path.exists(path):
                with open(path, "rb") as f:
                    data = tomllib.load(f)
                sa = data.get("gcp_service_account")
                if sa:
                    # If the TOML table is a plain dict (already parsed), just return it
                    return sa, f"file:{path}"
        except Exception as e:
            # show but keep trying others
            st.sidebar.warning(f"Could not parse secrets at {path}: {e}")
    return None, None

def make_bq_client():
    """
    Build a BigQuery client, trying sources in this order:
      A) st.secrets['gcp_service_account']
      B) secrets.toml on disk (HOME and CWD)
      C) GOOGLE_APPLICATION_CREDENTIALS
      D) Local hardcoded path (your laptop only)
    Returns: (client, source_str)
    """
    # A) Streamlit Secrets (Cloud or local .streamlit/secrets.toml recognized by Streamlit)
    sa_info = None
    try:
        sa_info = st.secrets.get("gcp_service_account", None)
    except Exception:
        sa_info = None

    if sa_info:
        if isinstance(sa_info, str):
            sa_info = json.loads(sa_info)  # if pasted as a raw JSON string
        creds = service_account.Credentials.from_service_account_info(sa_info)
        return bigquery.Client(credentials=creds, project=creds.project_id), "secrets:gcp_service_account"

    # B) Directly read secrets.toml from disk (HOME and CWD)
    sa_info, src = _load_sa_from_toml_files()
    if sa_info:
        # keys in TOML table are already parsed as a dict
        creds = service_account.Credentials.from_service_account_info(sa_info)
        return bigquery.Client(credentials=creds, project=creds.project_id), src

    # # C) Env var (local dev)
    # gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    # if gac and os.path.exists(gac):
    #     return bigquery.Client(), f"env:GOOGLE_APPLICATION_CREDENTIALS={gac}"

    # # D) Local fallback (only for your laptop)
    # LOCAL_SA_PATH = r"C:\Users\vinolin_delphin_spic\Documents\Credentials\vinolin_delphin_spicemoney-dwh_new.json"
    # if os.path.exists(LOCAL_SA_PATH):
    #     creds = service_account.Credentials.from_service_account_file(LOCAL_SA_PATH)
    #     return bigquery.Client(credentials=creds, project=creds.project_id), f"local:{LOCAL_SA_PATH}"

    raise RuntimeError(
        "No BigQuery credentials found.\n"
        "Place secrets.toml in HOME or CWD, set GOOGLE_APPLICATION_CREDENTIALS, "
        "or update LOCAL_SA_PATH."
    )

def bq_healthcheck(show=False):
    try:
        client, source = make_bq_client()
        if show:
            st.sidebar.info(f"BigQuery auth source: **{source}**")
        client.query("SELECT 1").result()  # smoke test
        if show:
            st.sidebar.success(f"BigQuery OK (project: {client.project})")
        return client
    except Exception as e:
        # keep this visible only when debugging
        if show:
            st.sidebar.error(f"BigQuery error: {e}")
            st.exception(e)
        else:
            st.error("BigQuery configuration error. Enable SHOW_DEBUG for details.")
        st.stop()




BQ_CLIENT = bq_healthcheck(show=SHOW_DEBUG)





def fmt_int(x):   return "—" if x is None or pd.isna(x) else f"{int(x):,}"
def fmt_lakh_from_rupees(x):
    if x is None or pd.isna(x): return "—"
    return f"{x/100000:,.2f} L"
def fmt_lakh_value(x):
    if x is None or pd.isna(x): return "—"
    return f"{x:,.2f} L"

KPI_CONFIG = {
    "Trxn_SMAs": {
        "value_col": "Trxn_SMAs",
        "unit_name": "Transacting SMAs",
        "unit_fmt": fmt_int,
        "bins": [0, 3, 8, 15, 20, 25, 35, 50, 10**9],
        "colors": R2G8,
        "sql": """
        WITH all_pincodes AS (
          SELECT DISTINCT pincode
          FROM `spicemoney-dwh.analytics_dwh.v_pincode_master`
        ),
        trxn_sma_data AS (
          SELECT pincode, COUNT(DISTINCT agent_id) AS Trxn_SMAs
          FROM (
            SELECT t1.agent_id, t2.final_pincode AS pincode
            FROM (
              SELECT agent_id
              FROM `spicemoney-dwh.analytics_dwh.csp_monthly_timeline_with_tu`
              WHERE month_year = @month AND total_gtv_amt > 0
            ) AS t1
            LEFT JOIN `spicemoney-dwh.analytics_dwh.v_client_pincode` AS t2
              ON t1.agent_id = t2.retailer_id
            {state_clause}  -- WHERE t2.final_state = @state
          )
          GROUP BY pincode
        )
        SELECT t1.pincode, COALESCE(Trxn_SMAs,0) AS Trxn_SMAs
        FROM all_pincodes AS t1
        LEFT JOIN trxn_sma_data AS t2
          ON t1.pincode = t2.pincode
        """
    },
    "AEPS_GTV_IN_LACS": {
        "value_col": "AEPS_GTV_IN_LACS",
        "unit_name": "AEPS GTV (Lakhs)",
        "unit_fmt": fmt_lakh_value,
        "bins": [0, 2, 5, 10, 15, 20, 25, 30, 50, 100, 10**9],
        "colors": ["#8B0000","#B22222","#FF0000","#FF4500","#FF7F00",
                   "#FFA500","#FFD700","#90EE90","#32CD32","#006400"],
        "sql": """
        WITH all_pincodes AS (
          SELECT DISTINCT pincode, state
          FROM `spicemoney-dwh.analytics_dwh.v_pincode_master`
        ),
        aeps_gtv_data AS (
          SELECT pincode, SUM(AEPS_GTV) AS AEPS_GTV
          FROM (
            SELECT t1.agent_id, AEPS_GTV, t2.final_pincode AS pincode
            FROM (
              SELECT agent_id, aeps_gtv_success AS AEPS_GTV
              FROM `spicemoney-dwh.analytics_dwh.csp_monthly_timeline_with_tu`
              WHERE month_year = @month AND total_gtv_amt > 0
            ) AS t1
            LEFT JOIN `spicemoney-dwh.analytics_dwh.v_client_pincode` AS t2
              ON t1.agent_id = t2.retailer_id
            {state_clause}  -- WHERE t2.final_state = @state
          )
          GROUP BY pincode
        )
        SELECT t1.pincode,
               ROUND(COALESCE(AEPS_GTV,0)/100000, 2) AS AEPS_GTV_IN_LACS
        FROM all_pincodes AS t1
        LEFT JOIN aeps_gtv_data AS t2
          ON t1.pincode = t2.pincode
        """
    },
    "CMS_GTV": {
        "value_col": "CMS_GTV",
        "unit_name": "CMS GTV (Lakhs)",
        "unit_fmt": fmt_lakh_from_rupees,   # rupees -> show as Lakhs
        "bins": [0, 2e5, 5e5, 1e6, 1.5e6, 2e6, 3e6, 5e6, 1e7, 1e12],
        "colors": ["#8B0000","#B22222","#FF0000","#FF7F00","#FFD700",
                   "#ADFF2F","#90EE90","#32CD32","#006400"],
        "sql": """
        WITH all_pincodes AS (
          SELECT DISTINCT pincode
          FROM `spicemoney-dwh.analytics_dwh.v_pincode_master`
        ),
        cms_gtv_data AS (
          SELECT pincode, SUM(CMS_GTV) AS CMS_GTV
          FROM (
            SELECT t1.agent_id, CMS_GTV, t2.final_pincode AS pincode
            FROM (
              SELECT agent_id, cms_gtv_success AS CMS_GTV
              FROM `spicemoney-dwh.analytics_dwh.csp_monthly_timeline_with_tu`
              WHERE month_year = @month AND total_gtv_amt > 0
            ) AS t1
            LEFT JOIN `spicemoney-dwh.analytics_dwh.v_client_pincode` AS t2
              ON t1.agent_id = t2.retailer_id
            {state_clause}  -- WHERE t2.final_state = @state
          )
          GROUP BY pincode
        )
        SELECT t1.pincode, COALESCE(CMS_GTV,0) AS CMS_GTV
        FROM all_pincodes AS t1
        LEFT JOIN cms_gtv_data AS t2
          ON t1.pincode = t2.pincode
        """
    },
}

STATES = [
    "All States",'UTTAR PRADESH',
'TAMIL NADU',
'DADRA & NAGAR HAVELI',
'DELHI_NCR',
'HARYANA',
'PUNJAB',
'MADHYA PRADESH',
'CHATTISGARH',
'TELANGANA',
'ANDHRA PRADESH',
'PONDICHERRY',
'WEST BENGAL',
'NAGALAND',
'JAMMU & KASHMIR',
'ASSAM',
'MANIPUR',
'ANDAMAN & NICOBAR ISLANDS',
'LAKSHADWEEP',
'GUJARAT',
'ODISHA',
'JHARKHAND',
'HIMACHAL PRADESH',
'UTTARAKHAND',
'KERALA',
'SIKKIM',
'MIZORAM',
'DAMAN & DIU',
'GOA',
'RAJASTHAN',
'MAHARASHTRA',
'KARNATAKA',
'TRIPURA',
'BIHAR',
'ARUNACHAL PRADESH',
'MEGHALAYA'
]

# =============== Auth & Cache ===============



import os, json
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


# ====== BigQuery Client (robust) ======
import os, json
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


# Build once, show status in the sidebar, fail fast if broken
# BQ_CLIENT = bq_healthcheck()

def get_bq_client():
    """Back-compat: return the verified, singleton BigQuery client."""
    return BQ_CLIENT


# def get_bq_client():
#     """
#     Order of credential sources:
#       A) st.secrets["gcp_service_account"]  (Cloud OR local .streamlit/secrets.toml)
#       B) GOOGLE_APPLICATION_CREDENTIALS env var (local)
#       C) Local hardcoded path (last resort for your laptop)
#     """
#     # A) Streamlit secrets (safe even if secrets.toml doesn't exist)
#     sa_info = None
#     try:
#         sa_info = st.secrets.get("gcp_service_account", None)
#         print("sa_infoe:", sa_info)
#     except Exception:
#         sa_info = None

#     if sa_info:
#         if isinstance(sa_info, str):   # allow pasting raw JSON string
#             sa_info = json.loads(sa_info)
#         creds = service_account.Credentials.from_service_account_info(sa_info)
#         return bigquery.Client(credentials=creds, project=creds.project_id)

    # B) Env var (local dev): set once in the shell before running streamlit
    # gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    # if gac and os.path.exists(gac):
    #     return bigquery.Client()  # google lib reads the JSON from env var

    # C) Local file path (your machine only). Change to your real path:


    # credentials = service_account.Credentials.from_service_account_file(
    # r'C:\Users\vinolin.delphin_spic\Documents\Credentials\vinolin_delphin_spicemoney-dwh_new.json')
    # client = bigquery.Client(credentials= credentials,project=credentials.project_id)

    # return client

    # LOCAL_SA_PATH = r"C:\Users\vinolin_delphin_spic\Documents\Credentials\vinolin_delphin_spicemoney-dwh_new.json"
    # if os.path.exists(LOCAL_SA_PATH):
    #     creds = service_account.Credentials.from_service_account_file(LOCAL_SA_PATH)
    #     return bigquery.Client(credentials=creds, project=creds.project_id)

    # raise RuntimeError(
    #     "No BigQuery credentials found.\n"
    #     "Add gcp_service_account to st.secrets OR set GOOGLE_APPLICATION_CREDENTIALS OR point to a local JSON path."
    # )


def normalize_pin_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.extract(r"(\d{6})", expand=False)

@st.cache_data(show_spinner=False)
def load_geojson(path: str, simplify_m: int):
    try:
        gdf = gpd.read_file(path, engine="pyogrio")
    except Exception:
        gdf = gpd.read_file(path)

    # Detect PIN column
    def _n(x): return re.sub(r"[^a-z0-9]", "", x.lower())
    props = [c for c in gdf.columns if c != "geometry"]
    cand = {_n(c): c for c in props}
    pin_col = None
    for k in ["pincode","pin","postalcode","postcode"]:
        if k in cand: pin_col = cand[k]; break
    if pin_col is None:
        for c in props:
            if gdf[c].astype(str).str.fullmatch(r"\d{6}", na=False).mean() > 0.6:
                pin_col = c; break
    if pin_col is None:
        raise ValueError("Could not detect a 6-digit PIN column in GeoJSON.")

    gdf[pin_col] = normalize_pin_series(gdf[pin_col])
    gdf = gdf.dropna(subset=[pin_col])
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326, allow_override=True)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    if simplify_m and simplify_m > 0:
        g2 = gdf.to_crs(epsg=3857)
        g2["geometry"] = g2.geometry.simplify(simplify_m, preserve_topology=True)
        gdf = g2.to_crs(epsg=4326)
    return gdf, pin_col

@st.cache_data(show_spinner=False)
def run_query_cached(sql: str, month_date: str, state_name: str) -> pd.DataFrame:
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("month", "DATE", month_date),
            bigquery.ScalarQueryParameter("state", "STRING", state_name),
        ]
    )
    return get_bq_client().query(sql, job_config=job_cfg).result().to_dataframe(create_bqstorage_client=False, progress_bar_type=None)

def run_query(kpi_key: str, month_date: str, state_name: str) -> pd.DataFrame:
    cfg = KPI_CONFIG[kpi_key]
    sql = cfg["sql"].format(
        state_clause="" if state_name == "All States" else "WHERE t2.final_state = @state"
    )
    df = run_query_cached(sql, month_date, state_name)
    if "pincode" not in df.columns:
        raise ValueError("Result must include 'pincode'.")
    df["pincode"] = normalize_pin_series(df["pincode"])
    return df

# =============== UI ===============
st.set_page_config(page_title="PIN-code Level Map Generation Utility", layout="wide")
st.title("PIN-code Level Map Generation Utility")

# Build last-12-months dropdown (DESC order)
def last_12_months_desc():
    first = date.today().replace(day=1)
    months = [first - relativedelta(months=i) for i in range(12)]  # recent -> older
    labels = [f"{calendar.month_abbr[m.month]} {m.year}" for m in months]
    values = [m.strftime("%Y-%m-01") for m in months]
    return labels, values

labels, values = last_12_months_desc()

# Session state for saved map & meta
if "last_map_html" not in st.session_state:
    st.session_state.last_map_html = None
    st.session_state.last_map_title = ""
if "last_map_meta" not in st.session_state:
    st.session_state.last_map_meta = None
if "pending_changes" not in st.session_state:
    st.session_state.pending_changes = True

def mark_changed():
    st.session_state.pending_changes = True

with st.sidebar:
    st.header("Controls")
    kpi_key = st.selectbox("KPI", list(KPI_CONFIG.keys()), index=0, on_change=mark_changed)
    month_label = st.selectbox("Month", labels, index=0, on_change=mark_changed)  # most recent first
    month_param = values[labels.index(month_label)]
    state = st.selectbox("State", STATES, index=0, on_change=mark_changed)
    clicked = st.button("Generate map", type="primary")

def render_header_and_button():
    """Render title (left) and orange download button (right) above the map."""
    meta   = st.session_state.last_map_meta or {"kpi": "map", "month": "", "state": ""}
    fname  = f"{meta['kpi']}_{meta['month'].replace(' ', '-')}_{meta['state'].replace(' ', '-')}.html"
    title  = st.session_state.last_map_title

    left, right = st.columns([1, 0.22], vertical_alignment="center")
    with left:
        st.markdown(title)
    with right:
        st.markdown(
            """
            <style>
            div[data-testid="column"]:has(div[data-testid="stDownloadButton"]) { text-align: right; }
            div[data-testid="stDownloadButton"] > button {
                background-color: #ff7a00 !important;
                color: #ffffff !important;
                border: 0 !important;
                border-radius: 6px !important;
                padding: 0.5rem 1rem !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.download_button(
            "Download this map",
            data=st.session_state.last_map_html.encode("utf-8"),
            file_name=fname,
            mime="text/html",
            key="dl_map_top",
        )

# Show persisted map (if any) when filters haven’t changed
if st.session_state.last_map_html and not clicked and not st.session_state.pending_changes:
    render_header_and_button()
    st_html(st.session_state.last_map_html, height=780)

# Generate map only on click
if clicked:
    cfg = KPI_CONFIG[kpi_key]
    value_col = cfg["value_col"]; bins = cfg["bins"]; colors = cfg["colors"]
    unit_fmt = cfg["unit_fmt"]; unit_name = cfg["unit_name"]

    with st.spinner("Generating map…"):
        # Geo
        gdf, pin_col = load_geojson(GEOJSON_PATH, SIMPLIFY_TOLERANCE_M)
        # Data
        df = run_query(kpi_key, month_param, state)
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        g = gdf.merge(df[["pincode", value_col]], left_on=pin_col, right_on="pincode",
                      how="left", validate="m:1")
        g["_val_fmt"] = g[value_col].apply(unit_fmt)

        # View
        if state == "All States":
            center, zoom = [22.0, 79.0], 5
        else:
            bb = g.total_bounds
            center = [(bb[1]+bb[3])/2, (bb[0]+bb[2])/2]; zoom = 6

        # color fn
        def color_for_value(x, edges, cols):
            import math
            if x is None or (isinstance(x, float) and (math.isnan(x))) or x == 0:
                return "#d9d9d9"
            for hi, col in zip(edges[1:], cols):
                if x <= hi: return col
            return cols[-1]

        # Folium map
        m = folium.Map(location=center, zoom_start=zoom, tiles="cartodbpositron")
        folium.GeoJson(
            g[[pin_col, value_col, "_val_fmt", "geometry"]].to_json(),
            name="choropleth",
            style_function=lambda f: {
                "fillColor": color_for_value(f["properties"].get(value_col, None), bins, colors),
                "color": "black", "weight": 0.25, "fillOpacity": 0.88, "opacity": 0.7
            },
            highlight_function=lambda _: {"weight": 1.0, "color": "black"},
            tooltip=folium.GeoJsonTooltip(
                fields=[pin_col, "_val_fmt"],
                aliases=["PIN", unit_name],
                localize=True
            ),
        ).add_to(m)

        # -------- Legend: top-right, scrollable, never clipped --------
        legend_items = [("#d9d9d9", "0 / missing")]
        def _fmt_edge(v):
            if kpi_key == "AEPS_GTV_IN_LACS": return f"{v:.0f} L"
            if kpi_key == "Trxn_SMAs": return f"{int(v)}"
            return f"{v/100000:.0f} L"

        for i in range(1, len(bins)-1):
            legend_items.append((colors[i-1], f"{_fmt_edge(bins[i-1])} – {_fmt_edge(bins[i])}"))
        legend_items.append((colors[-1], f"> {_fmt_edge(bins[-2])}"))

        legend_html = f"""
        <div id="map-legend"
             style="
                position: absolute;
                top: 14px;
                right: 14px;
                z-index: 999999;
                background: white;
                padding: 10px 12px;
                border: 1px solid #ccc;
                border-radius: 6px;
                box-shadow: 0 2px 8px rgba(0,0,0,.15);
                font-size: 12px;
                line-height: 1.15;
                max-height: 38vh;
                overflow-y: auto;
             ">
          <b>{kpi_key} • {unit_name}</b><br>
          {''.join(f'<i style="background:{c};width:12px;height:12px;display:inline-block;margin-right:6px;opacity:0.9"></i>{t}<br>'
                   for c,t in legend_items)}
        </div>
        <style>
        @media (max-width: 700px) {{
          #map-legend {{ top: 56px; right: 8px; }}
        }}
        </style>
        """
        m.get_root().html.add_child(folium.Element(legend_html))
        # --------------------------------------------------------------

        # Save in session
        title_md = f"### {kpi_key} • {month_label} • {state}"
        html_str = m._repr_html_()
        st.session_state.last_map_title = title_md
        st.session_state.last_map_html  = html_str
        st.session_state.last_map_meta  = {"kpi": kpi_key, "month": month_label, "state": state}
        st.session_state.pending_changes = False

    # Header + map
    render_header_and_button()
    st_html(st.session_state.last_map_html, height=780)

# If nothing generated yet
if not st.session_state.last_map_html and not clicked:
    st.info("Choose KPI, month and state, then click **Generate map**.")
