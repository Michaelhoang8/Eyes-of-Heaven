from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard.queries import (CHECKPOINTS, calibration_table, checkpoint_outcome_statistics,
                               checkpoint_table, completed_market_summary, market_series, open_connection)

st.set_page_config(page_title="Bitcoin Up/Down Market History", layout="wide")
st.title("Bitcoin Up/Down Market History")


def style_chart(fig, *, y_title: str):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(20,24,32,1)",
        font_color="#FAFAFA",
        xaxis_title="Time (UTC)",
        yaxis_title=y_title,
        hovermode="x unified",
        margin=dict(l=45, r=20, t=25, b=45),
    )
    fig.update_xaxes(gridcolor="rgba(255,255,255,0.12)")
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.12)")
    return fig


def chart_sample(frame, max_points=1500):
    """Keep endpoints and evenly spaced observations for reliable SVG charts."""
    if len(frame) <= max_points:
        return frame
    step = max(1, (len(frame) - 1) // (max_points - 1))
    sampled = frame.iloc[::step]
    if sampled.index[-1] != frame.index[-1]:
        sampled = pd.concat([sampled, frame.iloc[[-1]]])
    return sampled

try:
    con = open_connection()
    summary = completed_market_summary(con)
except Exception as exc:
    st.error(f"Could not open the research database: {exc}")
    st.info("Start or restart the collector once so it can publish a dashboard snapshot: python -m collector.main")
    st.stop()

if summary.empty:
    st.info("No completed markets have been recorded yet. Keep the collector running through a market close.")
    st.stop()

up_wins = int((summary.winner == "Up").sum()); down_wins = int((summary.winner == "Down").sum()); resolved = up_wins + down_wins
metrics = st.columns(7)
for box, label, value in zip(metrics,
    ["Completed markets","Up wins","Down wins","Up win %","Down win %","Average BTC change","Median BTC change"],
    [len(summary),up_wins,down_wins,f"{up_wins/resolved*100:.1f}%" if resolved else "—",f"{down_wins/resolved*100:.1f}%" if resolved else "—",
     f"${summary.btc_change.mean():,.2f}",f"${summary.btc_change.median():,.2f}"]): box.metric(label,value)

st.subheader("Historical markets")
f1,f2,f3,f4 = st.columns(4)
dates = summary.start_time.dt.date
date_range = f1.date_input("Date range", value=(dates.min(), dates.max()))
winner = f2.selectbox("Winner", ["All","Up","Down"])
minimum = f3.number_input("Minimum BTC movement ($)", value=float(summary.btc_change.min()), step=10.0)
maximum = f4.number_input("Maximum BTC movement ($)", value=float(summary.btc_change.max()), step=10.0)
filtered = summary.copy()
if len(date_range)==2: filtered=filtered[(filtered.start_time.dt.date>=date_range[0])&(filtered.start_time.dt.date<=date_range[1])]
if winner != "All": filtered=filtered[filtered.winner==winner]
filtered=filtered[filtered.btc_change.between(minimum,maximum)]
display = filtered.rename(columns={"start_time":"Start","end_time":"End","winner":"Winner","btc_open":"BTC Open","btc_close":"BTC Close",
 "btc_change":"BTC Change $","btc_change_pct":"BTC Change %","btc_range":"BTC Range", **{f"{o}_{s}s":f"{o.title()} @ {s}s" for o in ('up','down') for s in CHECKPOINTS}})
st.dataframe(display[["Start","End","Winner","BTC Open","BTC Close","BTC Change $","BTC Change %","BTC Range"]+
 [f"{o} @ {s}s" for o in ("Up","Down") for s in CHECKPOINTS]], use_container_width=True, hide_index=True)

st.subheader("Select a market")
options = dict(zip(filtered.apply(lambda r:f"{r.start_time} — {r.winner} ({r.market_id[:10]}…)",axis=1),filtered.market_id))
if options:
    selected = st.selectbox("Historical market", options, label_visibility="collapsed"); market_id=options[selected]
    row=summary[summary.market_id==market_id].iloc[0]
    st.markdown("#### Market details")
    st.dataframe({"Start":[row.start_time],"End":[row.end_time],"Winner":[row.winner],"BTC open":[row.btc_open],"BTC close":[row.btc_close],
      "BTC change":[row.btc_change],"BTC high":[row.btc_high],"BTC low":[row.btc_low],"BTC range":[row.btc_range]},hide_index=True,use_container_width=True)
    btc,poly=market_series(con,market_id)
    st.markdown("#### BTC chart")
    if btc.empty:
        st.info("No BTC observations were collected during this market interval.")
    else:
        btc_chart=chart_sample(btc)
        fig=px.line(btc_chart,x="event_time",y="price",template="plotly_dark",render_mode="svg")
        if row.btc_open is not None:
            fig.add_hline(y=row.btc_open,line_dash="dash",annotation_text="Opening price")
        st.plotly_chart(style_chart(fig,y_title="BTC/USDT"),use_container_width=True)
    st.markdown("#### Polymarket chart")
    valid_poly=poly.dropna(subset=["midpoint"])
    if valid_poly.empty:
        st.info("No complete Polymarket bid/ask observations were collected during this market interval.")
    else:
        poly_chart=pd.concat([chart_sample(group) for _,group in valid_poly.groupby("outcome",sort=False)],ignore_index=True)
        poly_fig=px.line(poly_chart,x="event_time",y="midpoint",color="outcome",template="plotly_dark",render_mode="svg",
                         color_discrete_map={"Up":"#2ECC71","Down":"#FF5C5C"})
        poly_fig.update_yaxes(range=[0,1])
        st.plotly_chart(style_chart(poly_fig,y_title="Midpoint"),use_container_width=True)
    st.markdown("#### Checkpoint table"); st.dataframe(checkpoint_table(con,market_id),hide_index=True,use_container_width=True)

st.subheader("Win distribution")
st.write(f"Up: {up_wins} ({up_wins/resolved*100:.1f}%) · Down: {down_wins} ({down_wins/resolved*100:.1f}%)" if resolved else "No resolved markets")
st.subheader("BTC movement distribution")
st.plotly_chart(px.histogram(summary,x="btc_change_bps",labels={"btc_change_bps":"BTC change (basis points)"}),use_container_width=True)
st.dataframe(summary.btc_change_bps.describe()[["mean","50%","std","min","max"]].rename({"50%":"median"}).to_frame("basis points").T)
st.subheader("Checkpoint outcome statistics")
st.caption("Descriptive analysis only; this is not a profitability claim.")
st.dataframe(checkpoint_outcome_statistics(summary),hide_index=True,use_container_width=True)
st.subheader("Simple calibration table")
checkpoint=st.selectbox("Checkpoint",CHECKPOINTS,format_func=lambda x:f"{x} seconds remaining")
st.dataframe(calibration_table(summary,checkpoint),hide_index=True,use_container_width=True)
