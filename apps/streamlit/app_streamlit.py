from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

import altair as alt
import pandas as pd
import streamlit as st
from loguru import logger
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

from src.common.config import load_config, Config


# =========================
# 1) Config & DB engine
# =========================


@st.cache_resource(show_spinner=False)
def load_cfg() -> Config:
    """Load config once."""
    cfg = load_config()
    logger.info(
        "Loaded config | pg={}:{} db={} table={}",
        cfg.postgres.host,
        cfg.postgres.port,
        cfg.postgres.database,
        getattr(cfg.sink, "gold_table", "gold_reddit_posts"),
    )
    return cfg


@st.cache_resource(show_spinner=False)
def mk_engine(cfg: Config):
    """Create SQLAlchemy engine (cached)."""
    url = (
        f"postgresql+psycopg2://{cfg.postgres.user}:{cfg.postgres.password}"
        f"@{cfg.postgres.host}:{int(cfg.postgres.port)}/{cfg.postgres.database}"
    )
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=5)


# =========================
# 2) Data access (cached)
# =========================


@st.cache_data(show_spinner=True, ttl=5)
def read_gold(_engine, table: str, limit: Optional[int]) -> pd.DataFrame:
    """Read most-recent rows from Postgres with optional LIMIT (cached)."""
    lim_sql = f"LIMIT {int(limit)}" if (limit and limit > 0) else ""
    sql = text(
        f"""
        SELECT
            post_id, subreddit, created_utc, dt, title, text,
            interaction_rate, score_stress, label_stress,
            permalink, feature_version, model_version
        FROM {table}
        ORDER BY created_utc DESC
        {lim_sql}
        """
    )
    return pd.read_sql(sql, con=_engine)


def normalize_gold(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleanup & sorting."""
    if "created_utc" in df.columns:
        df["created_utc"] = pd.to_datetime(df["created_utc"], utc=True, errors="coerce")
    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    for col in ("label_stress", "interaction_rate", "score_stress"):
        if col not in df.columns:
            df[col] = pd.NA
    return df.sort_values("created_utc", ascending=False)


# =========================
# 3) UI: Filters & KPIs
# =========================


@dataclass
class UIFilters:
    start: date
    end: date
    sel_subs: List[str]
    th: float  # stress threshold


def filter_panel(df: pd.DataFrame) -> tuple[UIFilters, int, int]:
    """Render sidebar once and return selected values (not filtered df)."""
    st.sidebar.header("Filters")

    refresh_sec = st.sidebar.select_slider(
        "Auto-refresh interval (sec)",
        options=[0, 5, 10, 15, 30, 60, 120],
        value=10,
        help="0 = no auto-refresh",
        key="refresh_sec_slider",
    )

    max_rows = st.sidebar.select_slider(
        "Max rows (query limit)",
        options=[1000, 5000, 10000, 20000, 50000],
        value=10000,
        help="Most-recent rows are queried first.",
        key="max_rows_slider",
    )

    if "dt" in df.columns and df["dt"].notna().any():
        min_dt = df["dt"].min()
        max_dt = df["dt"].max()
    else:
        min_dt = date.today() - timedelta(days=7)
        max_dt = date.today()

    start, end = st.sidebar.date_input(
        "Date range",
        value=(min_dt, max_dt),
        key="date_range_input",
    )

    subs = sorted(
        df.get("subreddit", pd.Series([], dtype="string")).dropna().unique().tolist()
    )
    default_subs = subs[: min(5, len(subs))] if subs else []
    sel_subs = st.sidebar.multiselect(
        "Subreddits",
        subs,
        default=default_subs,
        key="subs_multiselect",
    )

    # Lower default threshold so you still see most posts when scores are small
    th = st.sidebar.slider(
        "Stress score ≥",
        0.0,
        1.0,
        0.1,  # default 0.1 instead of 0.5
        0.05,
        key="stress_threshold_slider",
    )

    return (
        UIFilters(start=start, end=end, sel_subs=sel_subs, th=th),
        refresh_sec,
        max_rows,
    )


def filter_base(df: pd.DataFrame, f: UIFilters) -> pd.DataFrame:
    """Apply date + subreddit filters (no stress filter)."""
    out = df.copy()
    if "dt" in out.columns and f.start and f.end:
        out = out[(out["dt"] >= f.start) & (out["dt"] <= f.end)]
    if f.sel_subs:
        out = out[out["subreddit"].isin(f.sel_subs)]
    return out


def apply_stress_threshold(df: pd.DataFrame, th: float) -> pd.DataFrame:
    """Apply stress threshold on already filtered df."""
    out = df.copy()
    if "score_stress" in out.columns:
        out = out[out["score_stress"].fillna(0) >= th]
    return out


def kpi_block(df: pd.DataFrame, stress_threshold: float):
    """KPIs for overall + high-stress posts."""
    total = int(len(df))
    avg_score = (
        float(df["score_stress"].fillna(0).mean())
        if "score_stress" in df.columns
        else 0.0
    )
    stress_rate = (
        float(df["label_stress"].fillna(0).mean() * 100)
        if "label_stress" in df.columns
        else 0.0
    )

    high_cnt = 0
    high_pct = 0.0
    if "score_stress" in df.columns and total > 0:
        mask = df["score_stress"].fillna(0) >= stress_threshold
        high_cnt = int(mask.sum())
        high_pct = high_cnt / total * 100.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total posts", f"{total:,}")
    c2.metric("Avg stress score", f"{avg_score:.3f}")
    c3.metric("Label stress %", f"{stress_rate:.1f}%")
    c4.metric(
        f"High-stress (≥ {stress_threshold:.2f})",
        f"{high_cnt} ({high_pct:.1f}%)",
    )


# =========================
# 4) Advanced charts
# =========================


def heatmap_chart(df: pd.DataFrame):
    """Heatmap: posts per day × subreddit."""
    if df.empty or not {"dt", "subreddit"}.issubset(df.columns):
        st.info("Not enough data for heatmap.")
        return
    agg = (
        df.dropna(subset=["dt", "subreddit"])
        .groupby(["dt", "subreddit"])
        .size()
        .reset_index(name="count")
    )
    chart = (
        alt.Chart(agg)
        .mark_rect()
        .encode(
            x=alt.X("dt:T", title="Date"),
            y=alt.Y("subreddit:N", title="Subreddit"),
            color=alt.Color(
                "count:Q", title="Posts / day", scale=alt.Scale(scheme="blues")
            ),
            tooltip=["dt:T", "subreddit:N", "count:Q"],
        )
        .properties(height=360)
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


def wordcloud_chart(df: pd.DataFrame, max_words: int = 100):
    """Lightweight “word cloud” from titles (Altair text sizing)."""
    if df.empty or "title" not in df.columns:
        st.info("Not enough data for word cloud.")
        return

    stop = set(
        """a an and the or is are be been being i me my we our you your he she it they them his her its their to for of in on with from as by this that these those not no do does did have has had at just very really about into over out up down more most less least can could should would may might will""".split()
    )
    s = " ".join((df["title"].dropna().astype(str).values.tolist()))
    tokens = pd.Series(s).str.replace(r"[^A-Za-z0-9 ]", " ", regex=True).iloc[0].split()
    tokens = [t.lower() for t in tokens if len(t) > 2 and t.lower() not in stop]
    if not tokens:
        st.info("No tokens for word cloud.")
        return
    freq = pd.Series(tokens).value_counts().reset_index()
    freq.columns = ["word", "count"]
    freq = freq.head(max_words)
    freq["rank"] = range(1, len(freq) + 1)
    freq["row"] = (freq["rank"] - 1) // 10
    freq["col"] = (freq["rank"] - 1) % 10

    chart = (
        alt.Chart(freq)
        .mark_text()
        .encode(
            x=alt.X("col:O", axis=None),
            y=alt.Y("row:O", axis=None),
            size=alt.Size("count:Q", scale=alt.Scale(range=[10, 60]), legend=None),
            text="word:N",
            tooltip=["word:N", "count:Q"],
            color=alt.Color("count:Q", scale=alt.Scale(scheme="teals"), legend=None),
        )
        .properties(height=360)
    )
    st.altair_chart(chart, use_container_width=True)


def insights_panel(df: pd.DataFrame, top_k: int = 5):
    """Simple rule-based insights."""
    if df.empty:
        st.info("No data for insights.")
        return

    stress_avg = df["score_stress"].mean() if "score_stress" in df.columns else None
    top_subs = (
        df.groupby("subreddit")["score_stress"]
        .mean()
        .sort_values(ascending=False)
        .head(top_k)
        if {"subreddit", "score_stress"}.issubset(df.columns)
        else pd.Series(dtype=float)
    )
    top_posts = (
        df[["created_utc", "subreddit", "score_stress", "title", "permalink"]]
        .dropna(subset=["score_stress"])
        .sort_values("score_stress", ascending=False)
        .head(top_k)
        if "score_stress" in df.columns
        else df.head(0)
    )

    st.markdown("**Summary**")
    bullets = []
    if stress_avg is not None:
        bullets.append(f"- Average stress score: **{stress_avg:.3f}**")
    if not top_subs.empty:
        bullets.append("- Top subreddits by avg stress:")
        for s, v in top_subs.items():
            bullets.append(f"  - **r/{s}**: {v:.3f}")
    if not bullets:
        bullets.append("- Not enough metrics yet.")
    st.markdown("\n".join(bullets))

    st.markdown("**Most stressed recent posts**")
    if not top_posts.empty:
        st.dataframe(
            top_posts.reset_index(drop=True), use_container_width=True, height=300
        )
    else:
        st.info("No high-stress posts yet.")


# =========================
# 5) Main App
# =========================


def main():
    st.set_page_config(page_title="Reddit Stress Monitor (Postgres)", layout="wide")
    st.title("🧠 Reddit Stress Monitor")

    cfg = load_cfg()
    table = getattr(cfg.sink, "gold_table", "gold_reddit_posts")

    if "refresh_sec" not in st.session_state:
        st.session_state.refresh_sec = 10
    if "current_limit" not in st.session_state:
        st.session_state.current_limit = 10000

    engine = mk_engine(cfg)
    try:
        df = read_gold(engine, table=table, limit=st.session_state.current_limit)
    except Exception as e:
        st.error(f"❌ Failed to read Postgres: {e}")
        st.stop()

    df = normalize_gold(df)
    if df.empty:
        st.info("No data found yet. Please run your streaming jobs and refresh.")
        st.stop()

    # Sidebar once → get filters + limits
    filters, refresh_sec, max_rows = filter_panel(df)
    st.session_state.refresh_sec = refresh_sec

    # Re-query if user increased max_rows
    if max_rows > st.session_state.current_limit:
        try:
            st.session_state.current_limit = max_rows
            read_gold.clear()
            df = normalize_gold(read_gold(engine, table=table, limit=max_rows))
        except Exception as e:
            st.warning(f"Re-query failed: {e}")

    # Base filtered (date + subreddit, no stress threshold)
    base_df = filter_base(df, filters)
    # High-stress subset (may be empty)
    high_df = apply_stress_threshold(base_df, filters.th)
    # For charts/insights we prefer high_df, but fallback to base_df
    chart_df = high_df if not high_df.empty else base_df

    # Auto-refresh
    if refresh_sec > 0:
        st.caption("Auto-refresh is enabled via `streamlit-autorefresh`.")
        st_autorefresh(interval=refresh_sec * 1000, key="auto-refresh")

    # Tabs: Overview | Heatmap | Word Cloud | Insights
    tabs = st.tabs(["Overview", "Heatmap", "Word Cloud", "Insights"])

    with tabs[0]:
        # KPIs use all posts (within date + subs)
        kpi_block(base_df, filters.th)

        st.subheader("Posts over time")
        if "dt" in chart_df.columns and chart_df["dt"].notna().any():
            daily = chart_df.groupby("dt").size().reset_index(name="count")
            line = (
                alt.Chart(daily)
                .mark_line(point=True)
                .encode(
                    x=alt.X("dt:T", title="Date"), y=alt.Y("count:Q", title="Posts")
                )
                .properties(height=280)
            )
            st.altair_chart(line, use_container_width=True)

        st.subheader("Avg stress score by subreddit")
        if {"subreddit", "score_stress"}.issubset(
            chart_df.columns
        ) and not chart_df.empty:
            agg = (
                chart_df.dropna(subset=["subreddit"])
                .groupby("subreddit")["score_stress"]
                .mean()
                .sort_values(ascending=False)
                .reset_index()
            )
            if not agg.empty:
                bar = (
                    alt.Chart(agg)
                    .mark_bar()
                    .encode(
                        x=alt.X("score_stress:Q", title="Avg stress"),
                        y=alt.Y("subreddit:N", sort="-x", title="Subreddit"),
                        tooltip=[
                            "subreddit:N",
                            alt.Tooltip("score_stress:Q", format=".3f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(bar, use_container_width=True)

        st.subheader("Latest posts (all stress levels)")
        cols = ["created_utc", "subreddit", "score_stress", "title", "permalink"]
        show_cols = [c for c in cols if c in base_df.columns]
        st.dataframe(
            base_df[show_cols].reset_index(drop=True),
            use_container_width=True,
            height=420,
        )

    with tabs[1]:
        st.subheader("Heatmap (Posts per day × Subreddit)")
        heatmap_chart(chart_df)

    with tabs[2]:
        st.subheader("Word Cloud (titles)")
        wordcloud_chart(chart_df, max_words=100)

    with tabs[3]:
        st.subheader("Insights (rule-based summary)")
        insights_panel(chart_df)

    st.caption(
        f"Data source: Postgres → table '{table}' on "
        f"{cfg.postgres.host}:{cfg.postgres.port}/{cfg.postgres.database}"
    )
    st.caption("Built with Postgres, Altair & Streamlit")


if __name__ == "__main__":
    main()
