import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import time
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'satellites.duckdb')

st.set_page_config(
    page_title="ORBITAL — Satellite Tracker",
    page_icon="🛸",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;600;700&family=Space+Mono:wght@400;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Space Grotesk', sans-serif;
    background-color: #020818;
    color: #e2e8f0;
}
.main { background-color: #020818; }
.block-container { padding: 2rem 3rem; }

.hero {
    background: linear-gradient(135deg, #0d1b3e 0%, #020818 50%, #0a0f2e 100%);
    border: 1px solid #1e3a5f;
    border-radius: 16px;
    padding: 2.5rem 3rem;
    margin-bottom: 2rem;
    position: relative;
    overflow: hidden;
}
.hero::before {
    content: '';
    position: absolute;
    top: -50%; left: -50%;
    width: 200%; height: 200%;
    background: radial-gradient(circle at 30% 50%, rgba(56,189,248,0.05) 0%, transparent 50%),
                radial-gradient(circle at 70% 50%, rgba(139,92,246,0.05) 0%, transparent 50%);
    pointer-events: none;
}
.hero-title {
    font-family: 'Space Mono', monospace;
    font-size: 3rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    background: linear-gradient(90deg, #38bdf8, #818cf8, #c084fc);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin: 0;
    line-height: 1.1;
}
.hero-sub {
    font-size: 1rem;
    color: #64748b;
    margin-top: 0.5rem;
    font-family: 'Space Mono', monospace;
    letter-spacing: 0.1em;
    text-transform: uppercase;
}
.live-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(239,68,68,0.15);
    border: 1px solid rgba(239,68,68,0.4);
    color: #f87171;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-family: 'Space Mono', monospace;
    letter-spacing: 0.1em;
    margin-top: 1rem;
}
.live-dot {
    width: 6px; height: 6px;
    background: #ef4444;
    border-radius: 50%;
    animation: pulse 1.5s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.4; transform: scale(0.8); }
}
.kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 2rem; }
.kpi-card {
    background: linear-gradient(135deg, #0d1b3e, #0a0f2e);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 1.5rem;
    position: relative;
    overflow: hidden;
}
.kpi-card::after {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #38bdf8, #818cf8);
}
.kpi-label {
    font-size: 0.7rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-family: 'Space Mono', monospace;
    margin-bottom: 0.5rem;
}
.kpi-value {
    font-family: 'Space Mono', monospace;
    font-size: 2.2rem;
    font-weight: 700;
    color: #e2e8f0;
    line-height: 1;
}
.kpi-unit {
    font-size: 0.75rem;
    color: #38bdf8;
    margin-top: 0.25rem;
    font-family: 'Space Mono', monospace;
}
.section-title {
    font-family: 'Space Mono', monospace;
    font-size: 0.75rem;
    color: #38bdf8;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    margin-bottom: 1rem;
    display: flex;
    align-items: center;
    gap: 8px;
}
.section-title::before {
    content: '';
    display: inline-block;
    width: 20px; height: 1px;
    background: #38bdf8;
}
.divider { border: none; border-top: 1px solid #1e3a5f; margin: 2rem 0; }
</style>
""", unsafe_allow_html=True)

def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

SAT_COLORS = ["#38bdf8", "#f472b6", "#34d399", "#fbbf24", "#a78bfa"]

# ── HERO ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <p class="hero-title">ORBITAL</p>
    <p class="hero-sub">Real-time satellite & space debris tracking pipeline</p>
    <div class="live-badge"><div class="live-dot"></div> LIVE STREAM · Kafka → DuckDB → Streamlit</div>
</div>
""", unsafe_allow_html=True)

# ── KPIs ─────────────────────────────────────────────────────────────────────
try:
    con = get_con()
    total_sats  = con.execute("SELECT COUNT(DISTINCT name) FROM satellite_positions").fetchone()[0]
    total_pos   = con.execute("SELECT COUNT(*) FROM satellite_positions").fetchone()[0]
    avg_alt     = con.execute("SELECT ROUND(AVG(altitude_km), 1) FROM satellite_positions").fetchone()[0]
    last_update = con.execute("SELECT MAX(timestamp) FROM satellite_positions").fetchone()[0]
    con.close()
except:
    total_sats, total_pos, avg_alt, last_update = 0, 0, 0, "N/A"

st.markdown(f"""
<div class="kpi-grid">
    <div class="kpi-card">
        <div class="kpi-label">Satellites trackés</div>
        <div class="kpi-value">{total_sats}</div>
        <div class="kpi-unit">objets en orbite</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Positions enregistrées</div>
        <div class="kpi-value">{total_pos}</div>
        <div class="kpi-unit">events Kafka consommés</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Altitude moyenne</div>
        <div class="kpi-value">{avg_alt}</div>
        <div class="kpi-unit">km au-dessus du sol</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-label">Dernière mise à jour</div>
        <div class="kpi-value" style="font-size:1.1rem;padding-top:0.6rem">{str(last_update)[:19] if last_update else 'N/A'}</div>
        <div class="kpi-unit">UTC</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── VIZ 1 : GLOBE 3D ─────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Position actuelle · Globe interactif</div>', unsafe_allow_html=True)

try:
    con = get_con()
    df_map = con.execute("""
        SELECT name, latitude, longitude, altitude_km, timestamp
        FROM satellite_positions
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM satellite_positions sp2
            WHERE sp2.name = satellite_positions.name
        )
    """).df()
    con.close()
except:
    df_map = pd.DataFrame()

if not df_map.empty:
    fig_globe = go.Figure()

    for i, row in df_map.iterrows():
        n_points = 60
        t = np.linspace(-np.pi, np.pi, n_points)
        inc = abs(row["latitude"]) + 10
        lons = [(row["longitude"] + np.degrees(tt) * 1.5) % 360 - 180 for tt in t]
        lats = [inc * np.sin(tt + np.radians(row["latitude"])) for tt in t]
        fig_globe.add_trace(go.Scattergeo(
            lon=lons, lat=lats,
            mode="lines",
            line=dict(width=1.5, color=SAT_COLORS[i % len(SAT_COLORS)]),
            opacity=0.25,
            showlegend=False,
            hoverinfo="skip"
        ))

    fig_globe.add_trace(go.Scattergeo(
        lat=df_map["latitude"],
        lon=df_map["longitude"],
        text=df_map["name"],
        mode="markers+text",
        textposition="top center",
        textfont=dict(size=11, color="white", family="Space Mono"),
        marker=dict(
            size=16,
            color=SAT_COLORS[:len(df_map)],
            symbol="circle",
            line=dict(width=2, color="white"),
            opacity=0.95,
        ),
        hovertemplate="<b>%{text}</b><br>Lat: %{lat:.2f}°<br>Lon: %{lon:.2f}°<br>Alt: %{customdata:.0f} km<extra></extra>",
        customdata=df_map["altitude_km"],
        showlegend=False
    ))

    fig_globe.update_layout(
        geo=dict(
            projection_type="orthographic",
            projection_rotation=dict(lon=20, lat=30, roll=0),
            showland=True,       landcolor="#0d1b3e",
            showocean=True,      oceancolor="#020818",
            showlakes=True,      lakecolor="#0a1628",
            showcountries=True,  countrycolor="#1e3a5f", countrywidth=0.5,
            showcoastlines=True, coastlinecolor="#1e3a5f", coastlinewidth=0.8,
            bgcolor="#020818",
            lataxis=dict(showgrid=True, gridcolor="#0d1b3e", gridwidth=0.5),
            lonaxis=dict(showgrid=True, gridcolor="#0d1b3e", gridwidth=0.5),
        ),
        paper_bgcolor="#020818",
        plot_bgcolor="#020818",
        font=dict(color="#e2e8f0", family="Space Grotesk"),
        height=700,
        margin=dict(l=0, r=0, t=0, b=0),
    )
    st.plotly_chart(fig_globe, use_container_width=True)
    st.caption("💡 Clique et fais glisser pour faire tourner le globe · Scroll pour zoomer")
else:
    st.warning("En attente de données — lance le producer et le consumer.")

st.markdown('<hr class="divider">', unsafe_allow_html=True)

# ── VIZ 2 & 3 ────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section-title">Latitude au fil du temps</div>', unsafe_allow_html=True)
    try:
        con = get_con()
        df_traj = con.execute("""
            SELECT name, latitude, timestamp
            FROM satellite_positions ORDER BY timestamp ASC
        """).df()
        con.close()
        fig_traj = px.line(
            df_traj, x="timestamp", y="latitude", color="name",
            labels={"latitude": "Latitude (°)", "timestamp": "Heure", "name": "Satellite"},
            color_discrete_sequence=SAT_COLORS
        )
        fig_traj.update_layout(
            paper_bgcolor="#0d1b3e", plot_bgcolor="#0d1b3e",
            font=dict(color="#e2e8f0", family="Space Grotesk"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
            height=380, margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(gridcolor="#1e3a5f", title="Heure"),
            yaxis=dict(gridcolor="#1e3a5f", title="Latitude (°)")
        )
        st.plotly_chart(fig_traj, use_container_width=True)
    except Exception as e:
        st.error(str(e))

with col2:
    st.markdown('<div class="section-title">Distribution des latitudes survolées</div>', unsafe_allow_html=True)
    try:
        con = get_con()
        df_lat = con.execute("SELECT name, latitude FROM satellite_positions").df()
        con.close()
        fig_lat = px.histogram(
            df_lat, x="latitude", color="name", nbins=30,
            labels={"latitude": "Latitude (°)", "name": "Satellite"},
            color_discrete_sequence=SAT_COLORS
        )
        fig_lat.update_layout(
            paper_bgcolor="#0d1b3e", plot_bgcolor="#0d1b3e",
            font=dict(color="#e2e8f0", family="Space Grotesk"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
            height=380, margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(gridcolor="#1e3a5f"),
            yaxis=dict(gridcolor="#1e3a5f"),
            bargap=0.1
        )
        st.plotly_chart(fig_lat, use_container_width=True)
    except Exception as e:
        st.error(str(e))

st.markdown('<hr class="divider">', unsafe_allow_html=True)

# ── VIZ 4 & 5 ────────────────────────────────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.markdown('<div class="section-title">Statistiques par satellite</div>', unsafe_allow_html=True)
    try:
        con = get_con()
        df_stats = con.execute("""
            SELECT
                name                            AS Satellite,
                ROUND(AVG(altitude_km), 1)      AS "Alt. moy (km)",
                ROUND(MIN(altitude_km), 1)      AS "Alt. min (km)",
                ROUND(MAX(altitude_km), 1)      AS "Alt. max (km)",
                ROUND(AVG(latitude), 1)         AS "Lat. moy (°)",
                COUNT(*)                        AS "Nb positions"
            FROM satellite_positions
            GROUP BY name
            ORDER BY "Alt. moy (km)" DESC
        """).df()
        con.close()
        st.dataframe(df_stats, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(str(e))

with col4:
    st.markdown('<div class="section-title">Répartition par type d\'objet</div>', unsafe_allow_html=True)
    try:
        con = get_con()
        df_type = con.execute("""
            SELECT object_type AS Type, COUNT(*) AS Total
            FROM raw_satellites GROUP BY object_type
        """).df()
        con.close()
        fig_pie = px.pie(
            df_type, names="Type", values="Total",
            color_discrete_sequence=["#38bdf8", "#f472b6", "#34d399"],
            hole=0.5
        )
        fig_pie.update_traces(
            textfont=dict(family="Space Mono", color="white"),
            marker=dict(line=dict(color="#020818", width=3))
        )
        fig_pie.update_layout(
            paper_bgcolor="#0d1b3e",
            font=dict(color="#e2e8f0", family="Space Grotesk"),
            legend=dict(bgcolor="rgba(0,0,0,0)"),
            height=380, margin=dict(l=10, r=10, t=10, b=10),
            annotations=[dict(
                text="ORBITE", x=0.5, y=0.5,
                font=dict(size=14, color="#64748b", family="Space Mono"),
                showarrow=False
            )]
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    except Exception as e:
        st.error(str(e))

# ── FOOTER ───────────────────────────────────────────────────────────────────
st.markdown('<hr class="divider">', unsafe_allow_html=True)
st.markdown("""
<div style="display:flex;justify-content:space-between;align-items:center;color:#334155;font-family:'Space Mono',monospace;font-size:0.7rem;">
    <span>ORBITAL · ETL & Pipeline Orchestration · ESILV MSc A4 · 2026</span>
    <span>🔄 Auto-refresh · 10s</span>
</div>
""", unsafe_allow_html=True)

time.sleep(10)
st.rerun()