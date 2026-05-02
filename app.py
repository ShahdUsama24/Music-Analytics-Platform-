import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# ── Page Config ─────────────────────────────────────────────
st.set_page_config(page_title="Music Analytics", layout="wide")

# ── Custom UI ──────────────────────────────────────────────
st.markdown("""
<style>
:root {
  --bg:#0e1117;
  --card:#161b22;
  --text:#e6edf3;
  --muted:#8b949e;
  --border:#30363d;
  --accent:#4fffB0;
}
.card {
  background:var(--card);
  border:1px solid var(--border);
  border-radius:10px;
  padding:1.2rem;
  margin-bottom:1rem;
}
.hero-title {
  font-size:2.5rem;
  font-weight:300;
}
.metric {
  font-size:2rem;
  color:var(--accent);
}
</style>
""", unsafe_allow_html=True)

# ── Data ───────────────────────────────────────────────────
@st.cache_data
def generate_data(n=2000):
    rng = np.random.default_rng(42)
    songs = ["Blinding Lights","Shape of You","Levitating","Stay","Bad Guy"]
    artists = ["Weeknd","Ed Sheeran","Dua Lipa","Kid Laroi","Billie"]

    df = pd.DataFrame({
        "song": rng.choice(songs, n),
        "artist": rng.choice(artists, n),
        "plays": rng.integers(1, 100, n),
        "date": pd.date_range("2023-01-01", periods=n, freq="H")
    })
    return df

df = generate_data()

# ── Sidebar Controls ───────────────────────────────────────
st.sidebar.header("🎛️ Controls")

palette = st.sidebar.selectbox(
    "Choose Color Palette",
    ["Viridis", "Plasma", "Cividis", "Blues", "Greens"]
)

artist_filter = st.sidebar.selectbox(
    "Filter by artist",
    ["All"] + sorted(df["artist"].unique().tolist())
)

if artist_filter != "All":
    df = df[df["artist"] == artist_filter]

# ── Header ─────────────────────────────────────────────────
st.markdown('<div class="hero-title">🎧 Music Analytics Dashboard</div>', unsafe_allow_html=True)

# ── Metrics ────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

col1.markdown(f"""
<div class="card">
Total Plays
<div class="metric">{df['plays'].sum():,}</div>
</div>
""", unsafe_allow_html=True)

col2.markdown(f"""
<div class="card">
Songs
<div class="metric">{df['song'].nunique()}</div>
</div>
""", unsafe_allow_html=True)

col3.markdown(f"""
<div class="card">
Artists
<div class="metric">{df['artist'].nunique()}</div>
</div>
""", unsafe_allow_html=True)

# ── Aggregations ───────────────────────────────────────────
song_dist = df.groupby("song")["plays"].sum().reset_index()
artist_dist = df.groupby("artist")["plays"].sum().reset_index()

# ── Charts Layout ──────────────────────────────────────────
colA, colB = st.columns(2)

# 🍩 Donut Chart
fig_donut = px.pie(
    song_dist,
    names="song",
    values="plays",
    hole=0.5,
    color_discrete_sequence=px.colors.sequential.__dict__[palette]
)
fig_donut.update_layout(title="🍩 Song Distribution (Donut)")
colA.plotly_chart(fig_donut, use_container_width=True)

# 🥧 Pie Chart
fig_pie = px.pie(
    artist_dist,
    names="artist",
    values="plays",
    color_discrete_sequence=px.colors.sequential.__dict__[palette]
)
fig_pie.update_layout(title="🥧 Artist Distribution (Pie)")
colB.plotly_chart(fig_pie, use_container_width=True)

# 📊 Bar Chart
st.markdown("### 📊 Top Songs")

fig_bar = px.bar(
    song_dist.sort_values("plays", ascending=False),
    x="song",
    y="plays",
    color="plays",
    color_continuous_scale=palette
)
st.plotly_chart(fig_bar, use_container_width=True)

# 📈 Time Series
st.markdown("### 📈 Plays Over Time")

time_series = df.groupby(df["date"].dt.date)["plays"].sum().reset_index()

fig_line = px.line(
    time_series,
    x="date",
    y="plays",
    color_discrete_sequence=["#4fffB0"]
)
st.plotly_chart(fig_line, use_container_width=True)

# ── Data Table ─────────────────────────────────────────────
st.markdown("### 📋 Data Preview")
st.dataframe(df.head(100), use_container_width=True)

import streamlit.components.v1 as components

with open("background.html", "r", encoding="utf-8") as f:
    html = f.read()

components.html(html, height=0)