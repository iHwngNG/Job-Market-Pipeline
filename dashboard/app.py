"""
Vietnam Job Market Intelligence Dashboard
==========================================
Streamlit dashboard querying PostgreSQL directly.
Works independently of dbt — queries analytics_jobs table in public schema.

Run:
    cd dashboard
    streamlit run app.py
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

# ══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG & CUSTOM CSS
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="VN Job Market Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Auto-refresh every 60 seconds to pick up new data after pipeline runs
st.markdown(
    '<meta http-equiv="refresh" content="60">',
    unsafe_allow_html=True,
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1A1F2E 0%, #252B3B 100%);
        border: 1px solid rgba(108, 99, 255, 0.2);
        border-radius: 16px;
        padding: 24px;
        text-align: center;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(108, 99, 255, 0.15);
    }
    .kpi-value {
        font-size: 2.4rem;
        font-weight: 700;
        background: linear-gradient(135deg, #6C63FF, #48C6EF);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #8B8FA3;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 4px;
    }
    .section-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #FAFAFA;
        margin-bottom: 4px;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(108, 99, 255, 0.3);
    }
    .section-sub {
        font-size: 0.8rem;
        color: #6B7280;
        margin-bottom: 16px;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #111827 0%, #1A1F2E 100%);
    }
    header[data-testid="stHeader"] {
        background: rgba(14, 17, 23, 0.95);
        backdrop-filter: blur(10px);
    }
    hr { border-color: rgba(108, 99, 255, 0.15) !important; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# DB CONNECTION
# ══════════════════════════════════════════════════════════════════════════════

COLORS = [
    "#6C63FF", "#48C6EF", "#F472B6", "#34D399", "#FBBF24",
    "#FB923C", "#A78BFA", "#22D3EE", "#F87171", "#818CF8",
]

PLOTLY_LAYOUT = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {"family": "Inter", "color": "#FAFAFA", "size": 12},
    "margin": {"l": 20, "r": 20, "t": 40, "b": 20},
    "xaxis": {"gridcolor": "rgba(255,255,255,0.05)"},
    "yaxis": {"gridcolor": "rgba(255,255,255,0.05)"},
}


@st.cache_resource
def get_engine():
    """Create SQLAlchemy engine."""
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "job_market")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
    url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return create_engine(url)


@st.cache_data(ttl=60)
def run_query(query: str) -> pd.DataFrame:
    """Execute SQL and return DataFrame (cached 5 min)."""
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def table_exists(table_name: str) -> bool:
    """Check if a table or view exists in the database."""
    try:
        df = run_query(
            f"SELECT 1 FROM information_schema.tables "
            f"WHERE table_name = '{table_name}' LIMIT 1"
        )
        return len(df) > 0
    except Exception:
        return False


def render_kpi(label: str, value: str):
    """Render a single KPI card."""
    st.markdown(f"""
    <div class="kpi-card">
        <p class="kpi-value">{value}</p>
        <p class="kpi-label">{label}</p>
    </div>
    """, unsafe_allow_html=True)


def section_header(title: str, subtitle: str = ""):
    """Render a styled section header."""
    st.markdown(
        f'<p class="section-header">{title}</p>',
        unsafe_allow_html=True,
    )
    if subtitle:
        st.markdown(
            f'<p class="section-sub">{subtitle}</p>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# CHECK DATA AVAILABILITY
# ══════════════════════════════════════════════════════════════════════════════

has_analytics = table_exists("analytics_jobs")

if not has_analytics:
    st.markdown(
        "<h1 style='text-align:center; margin-top:100px;'>📊 VN Job Market "
        "Intelligence</h1>",
        unsafe_allow_html=True,
    )
    st.warning(
        "⚠️ **Table `analytics_jobs` chưa tồn tại.**\n\n"
        "Hãy chạy pipeline trước:\n"
        "1. Trigger **dag_crawl_and_ingest** trong Airflow UI (:8081)\n"
        "2. Chờ Kafka Consumer ghi vào `raw_jobs`\n"
        "3. Trigger **dag_transform_and_model** (PySpark → dbt)\n"
        "4. Refresh trang này."
    )
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR FILTERS
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## 📊 VN Job Market")
    st.markdown("**Intelligence Dashboard**")
    st.markdown("---")

    locations_df = run_query(
        "SELECT DISTINCT locations FROM analytics_jobs "
        "WHERE locations IS NOT NULL ORDER BY locations"
    )
    location_options = ["All"] + locations_df["locations"].tolist()

    roles_df = run_query(
        "SELECT DISTINCT role FROM analytics_jobs "
        "WHERE role IS NOT NULL ORDER BY role"
    )
    role_options = ["All"] + roles_df["role"].tolist()

    selected_location = st.selectbox("📍 Location", location_options, index=0)
    selected_role = st.selectbox("💼 Role", role_options, index=0)

    st.markdown("---")
    st.markdown(
        "<p style='color:#6B7280; font-size:0.75rem;'>"
        "Data refreshed every 5 min<br>Source: ITviec</p>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# FILTER HELPER
# ══════════════════════════════════════════════════════════════════════════════

def build_where(
    location_col: str = "locations",
    role_col: str = "role",
    extra: str = "",
):
    """Build SQL WHERE clause from sidebar filters."""
    conditions = []
    if selected_location != "All":
        safe = selected_location.replace("'", "''")
        conditions.append(f"{location_col} = '{safe}'")
    if selected_role != "All":
        safe = selected_role.replace("'", "''")
        conditions.append(f"{role_col} = '{safe}'")
    if extra:
        conditions.append(extra)
    return ("WHERE " + " AND ".join(conditions)) if conditions else ""


# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════

st.markdown(
    "<h1 style='text-align:center; font-weight:700; "
    "background: linear-gradient(135deg, #6C63FF, #48C6EF); "
    "-webkit-background-clip: text; -webkit-text-fill-color: transparent;'>"
    "🇻🇳 Vietnam Job Market Intelligence</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "<p style='text-align:center; color:#6B7280; margin-bottom:24px;'>"
    "Real-time insights from Vietnam's top IT job platforms</p>",
    unsafe_allow_html=True,
)

# ══════════════════════════════════════════════════════════════════════════════
# ROW 1: KPI CARDS
# ══════════════════════════════════════════════════════════════════════════════

where = build_where()
kpi = run_query(f"""
    SELECT
        COUNT(DISTINCT job_id)  AS total_jobs,
        COUNT(DISTINCT company) AS total_companies,
        COUNT(DISTINCT role)    AS total_roles,
        ROUND(AVG(
            CASE
                WHEN min_salary IS NOT NULL AND max_salary IS NOT NULL
                    THEN (min_salary + max_salary) / 2.0
                WHEN min_salary IS NOT NULL THEN min_salary
                WHEN max_salary IS NOT NULL THEN max_salary
            END
        ), 0) AS avg_salary
    FROM analytics_jobs
    {where}
""")

c1, c2, c3, c4 = st.columns(4)
with c1:
    render_kpi("Total Jobs", f"{int(kpi['total_jobs'].iloc[0]):,}")
with c2:
    render_kpi("Companies Hiring", f"{int(kpi['total_companies'].iloc[0]):,}")
with c3:
    render_kpi("Unique Roles", f"{int(kpi['total_roles'].iloc[0]):,}")
with c4:
    avg_sal = kpi["avg_salary"].iloc[0]
    render_kpi("Avg Salary", f"${int(avg_sal):,}" if pd.notna(avg_sal) else "N/A")

st.markdown("<br>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# ROW 2: TOP SKILLS + TOP ROLES
# ══════════════════════════════════════════════════════════════════════════════

col_left, col_right = st.columns(2)

# --- Top 10 Skills ---
with col_left:
    section_header("🔥 Top 10 In-Demand Skills", "Unique job postings per skill")

    where_sk = build_where()
    skills_df = run_query(f"""
        SELECT skill_name, COUNT(DISTINCT job_id) AS total_demand
        FROM (
            SELECT job_id, UNNEST(string_to_array(standardized_skills, ',')) AS skill_name,
                   role, locations
            FROM analytics_jobs
            WHERE standardized_skills IS NOT NULL AND standardized_skills != ''
        ) s
        {where_sk.replace('locations', 's.locations').replace('role', 's.role') if where_sk else ''}
        GROUP BY skill_name
        ORDER BY total_demand DESC
        LIMIT 10
    """)

    if not skills_df.empty:
        fig = px.bar(
            skills_df, x="total_demand", y="skill_name", orientation="h",
            color="total_demand",
            color_continuous_scale=["#1A1F2E", "#6C63FF", "#48C6EF"],
        )
        fig.update_layout(
            **PLOTLY_LAYOUT, showlegend=False, coloraxis_showscale=False,
            height=400,
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_traces(
            marker_line_width=0, texttemplate="%{x}",
            textposition="outside", textfont_size=11,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No skills data available.")

# --- Top 10 Roles ---
with col_right:
    section_header("💼 Top 10 Roles by Demand", "Most sought-after positions")

    # Don't filter by role here (would defeat the purpose)
    where_loc = ""
    if selected_location != "All":
        safe = selected_location.replace("'", "''")
        where_loc = f"WHERE locations = '{safe}'"

    roles_df = run_query(f"""
        SELECT role, COUNT(DISTINCT job_id) AS job_count
        FROM analytics_jobs
        {where_loc}
        GROUP BY role
        ORDER BY job_count DESC
        LIMIT 10
    """)

    if not roles_df.empty:
        fig = px.bar(
            roles_df, x="job_count", y="role", orientation="h",
            color="job_count",
            color_continuous_scale=["#1A1F2E", "#F472B6", "#FBBF24"],
        )
        fig.update_layout(
            **PLOTLY_LAYOUT, showlegend=False, coloraxis_showscale=False,
            height=400,
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_traces(
            marker_line_width=0, texttemplate="%{x}",
            textposition="outside", textfont_size=11,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No roles data available.")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# ROW 3: LEVEL DISTRIBUTION + SALARY BY ROLE
# ══════════════════════════════════════════════════════════════════════════════

col_l2, col_r2 = st.columns(2)

with col_l2:
    section_header("📈 Hiring by Experience Level", "Job count per seniority tier")

    level_df = run_query(f"""
        SELECT level, COUNT(DISTINCT job_id) AS job_count
        FROM analytics_jobs
        {build_where()}
        GROUP BY level
        ORDER BY job_count DESC
    """)

    if not level_df.empty:
        level_order = [
            "Intern", "Fresher", "Junior", "Middle",
            "Senior", "Lead", "Manager", "Not Specified",
        ]
        level_df["level"] = pd.Categorical(
            level_df["level"], categories=level_order, ordered=True,
        )
        level_df = level_df.sort_values("level").dropna(subset=["level"])

        fig = px.pie(
            level_df, values="job_count", names="level",
            color_discrete_sequence=COLORS, hole=0.45,
        )
        fig.update_layout(
            **PLOTLY_LAYOUT, height=400,
            legend={
                "orientation": "h", "yanchor": "bottom", "y": -0.15,
                "xanchor": "center", "x": 0.5,
                "font": {"size": 11}, "bgcolor": "rgba(0,0,0,0)",
            },
        )
        fig.update_traces(
            textposition="inside", textinfo="label+percent",
            textfont_size=11,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No level data available.")

with col_r2:
    section_header("💰 Average Salary by Role", "USD currency only")

    salary_where = build_where(extra="currency = 'USD'")
    if not salary_where:
        salary_where = "WHERE currency = 'USD'"

    salary_df = run_query(f"""
        SELECT role,
               ROUND(AVG((COALESCE(min_salary,0) + COALESCE(max_salary,0)) / 2.0), 0)
                   AS avg_salary,
               COUNT(DISTINCT job_id) AS job_count
        FROM analytics_jobs
        {salary_where}
        GROUP BY role
        ORDER BY avg_salary DESC
        LIMIT 10
    """)

    if not salary_df.empty:
        fig = px.bar(
            salary_df, x="avg_salary", y="role", orientation="h",
            color="avg_salary",
            color_continuous_scale=["#1A1F2E", "#34D399", "#FBBF24"],
            hover_data=["job_count"],
        )
        fig.update_layout(
            **PLOTLY_LAYOUT, showlegend=False, coloraxis_showscale=False,
            height=400,
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_traces(
            marker_line_width=0, texttemplate="$%{x:,.0f}",
            textposition="outside", textfont_size=11,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No USD salary data available.")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# ROW 4: TOP COMPANIES + SALARY BY LEVEL
# ══════════════════════════════════════════════════════════════════════════════

col_l3, col_r3 = st.columns(2)

with col_l3:
    section_header("🏢 Top 10 Hiring Companies", "Companies with most open positions")

    companies_df = run_query("""
        SELECT company,
               COUNT(DISTINCT job_id) AS total_jobs,
               COUNT(DISTINCT role)   AS unique_roles,
               ROUND(AVG(
                   CASE WHEN min_salary IS NOT NULL AND max_salary IS NOT NULL
                        THEN (min_salary + max_salary) / 2.0 END
               ), 0) AS avg_offered_salary
        FROM analytics_jobs
        WHERE company IS NOT NULL AND company != ''
        GROUP BY company
        ORDER BY total_jobs DESC
        LIMIT 10
    """)

    if not companies_df.empty:
        fig = px.bar(
            companies_df, x="total_jobs", y="company", orientation="h",
            color="total_jobs",
            color_continuous_scale=["#1A1F2E", "#A78BFA", "#E879F9"],
            hover_data=["unique_roles", "avg_offered_salary"],
        )
        fig.update_layout(
            **PLOTLY_LAYOUT, showlegend=False, coloraxis_showscale=False,
            height=400,
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_traces(
            marker_line_width=0, texttemplate="%{x}",
            textposition="outside", textfont_size=11,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No company data available.")

with col_r3:
    section_header("📊 Salary Range by Level", "Min / Avg / Max per level (USD)")

    sal_level_df = run_query("""
        SELECT level,
               ROUND(AVG(min_salary), 0) AS avg_min,
               ROUND(AVG(max_salary), 0) AS avg_max,
               ROUND(AVG((COALESCE(min_salary,0)+COALESCE(max_salary,0))/2.0), 0)
                   AS avg_mid
        FROM analytics_jobs
        WHERE currency = 'USD'
          AND level IS NOT NULL AND level != 'Not Specified'
          AND (min_salary IS NOT NULL OR max_salary IS NOT NULL)
        GROUP BY level
        ORDER BY avg_mid DESC
    """)

    if not sal_level_df.empty:
        level_order = [
            "Intern", "Fresher", "Junior", "Middle",
            "Senior", "Lead", "Manager",
        ]
        sal_level_df["level"] = pd.Categorical(
            sal_level_df["level"], categories=level_order, ordered=True,
        )
        sal_level_df = sal_level_df.sort_values("level").dropna(subset=["level"])

        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Min Salary", x=sal_level_df["level"],
            y=sal_level_df["avg_min"], marker_color="#6C63FF", opacity=0.7,
        ))
        fig.add_trace(go.Bar(
            name="Avg Salary", x=sal_level_df["level"],
            y=sal_level_df["avg_mid"], marker_color="#48C6EF",
        ))
        fig.add_trace(go.Bar(
            name="Max Salary", x=sal_level_df["level"],
            y=sal_level_df["avg_max"], marker_color="#34D399", opacity=0.7,
        ))
        fig.update_layout(
            **PLOTLY_LAYOUT, barmode="group", height=400,
            legend={
                "orientation": "h", "yanchor": "bottom", "y": 1.02,
                "xanchor": "center", "x": 0.5,
            },
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No salary level data available.")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# ROW 5: DATA TABLE
# ══════════════════════════════════════════════════════════════════════════════

section_header("📋 Job Listings Detail", "Browse individual job postings")

table_df = run_query(f"""
    SELECT job_id, title, role, level, company, locations,
           min_salary, max_salary, currency, posted_date, source
    FROM analytics_jobs
    {build_where()}
    ORDER BY posted_date DESC NULLS LAST
    LIMIT 100
""")

if not table_df.empty:
    st.dataframe(
        table_df,
        use_container_width=True,
        height=400,
        column_config={
            "job_id": st.column_config.TextColumn("Job ID", width="small"),
            "title": st.column_config.TextColumn("Title", width="large"),
            "role": st.column_config.TextColumn("Role"),
            "level": st.column_config.TextColumn("Level"),
            "company": st.column_config.TextColumn("Company"),
            "locations": st.column_config.TextColumn("Location"),
            "min_salary": st.column_config.NumberColumn("Min $", format="%d"),
            "max_salary": st.column_config.NumberColumn("Max $", format="%d"),
            "currency": st.column_config.TextColumn("Cur"),
            "posted_date": st.column_config.TextColumn("Posted"),
            "source": st.column_config.TextColumn("Source", width="small"),
        },
    )
else:
    st.info("No job listings match the current filters.")

# ══════════════════════════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    "<p style='text-align:center; color:#4B5563; font-size:0.75rem;'>"
    "Built with ❤️ using Streamlit • Pipeline: "
    "Playwright → Kafka → PostgreSQL → PySpark → dbt → Streamlit"
    "</p>",
    unsafe_allow_html=True,
)
