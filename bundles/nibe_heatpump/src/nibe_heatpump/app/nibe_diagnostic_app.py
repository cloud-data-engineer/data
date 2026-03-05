"""
NIBE Heat Pump — Diagnostic Suite
Three-tab Streamlit application:
  Tab 1: Dashboard  — daily aggregations, flow quality, superheat trends
  Tab 2: Agent Diagnosis — calls UC diagnostic tools, period comparison
  Tab 3: Raw LogViewer  — 5-second silver layer data with parameter selection
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from databricks.sdk.core import Config
import json
import os

# --- Connection ---
# Config() auto-detects credentials:
#   - On Databricks Apps: OAuth service principal (DATABRICKS_CLIENT_ID/SECRET injected)
#   - Locally: DATABRICKS_HOST + DATABRICKS_TOKEN env vars
_cfg = Config()

WAREHOUSE_HTTP_PATH = os.getenv(
    "DATABRICKS_HTTP_PATH",
    "/sql/1.0/warehouses/48f717bd780befa1",  # fallback for local dev
)

# Environment-based catalog routing (dev/prod)
_env = os.getenv("ENV_SCOPE", "dev")  # "dev" or "prod"
_GL = f"{_env}_gold.nibe"  # gold layer catalog.schema


@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=_cfg.host,
        http_path=WAREHOUSE_HTTP_PATH,
        access_token=_cfg.token,
    )


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)


# --- App Config ---
st.set_page_config(page_title="NIBE Diagnostics", layout="wide")
st.title("NIBE Heat Pump — Diagnostic Suite")

tab1, tab2, tab3 = st.tabs(["Dashboard", "Agent Diagnosis", "Raw LogViewer"])

# =====================================================
# TAB 1: DASHBOARD
# =====================================================
with tab1:
    col_days, col_spacer = st.columns([1, 5])
    with col_days:
        days_back = st.selectbox("Period", [7, 14, 30, 60, 90], index=1)

    df_daily = run_query(f"""
        SELECT * FROM {_GL}.gold_nibe_daily_summary
        WHERE log_date >= DATE_SUB(CURRENT_DATE(), {days_back})
        ORDER BY log_date
    """)

    if len(df_daily) == 0:
        st.warning("No data for selected period. All available data spans Jan-Feb 2026.")
        st.info("Try the Dashboard with all data: change period to 90 days or use the table below.")
        df_daily = run_query(f"""
            SELECT * FROM {_GL}.gold_nibe_daily_summary ORDER BY log_date
        """)

    if len(df_daily) > 0:
        # KPIs
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Outdoor Avg", f"{df_daily['outdoor_avg_c'].astype(float).mean():.1f}°C")
        delta_dt = (
            None if len(df_daily) < 2
            else f"{float(df_daily['delta_t_avg_k'].iloc[-1]) - float(df_daily['delta_t_avg_k'].iloc[-2]):+.1f}K"
        )
        k2.metric("ΔT Avg", f"{df_daily['delta_t_avg_k'].astype(float).mean():.1f}K", delta=delta_dt)
        k3.metric("Heater Total", f"{df_daily['heater_energy_kwh'].astype(float).sum():.0f} kWh")
        k4.metric("Neg Superheat", f"{df_daily['neg_superheat_pct'].astype(float).mean():.0f}%")
        k5.metric("Alarm (excl. defrost)", f"{df_daily['alarm_excl_183_pct'].astype(float).mean():.1f}%")

        # Temperature chart
        fig_temp = go.Figure()
        fig_temp.add_trace(go.Scatter(x=df_daily['log_date'], y=df_daily['outdoor_avg_c'].astype(float),
                                       name='Outdoor Avg', line=dict(color='blue')))
        fig_temp.add_trace(go.Scatter(x=df_daily['log_date'], y=df_daily['outdoor_min_c'].astype(float),
                                       name='Outdoor Min', line=dict(color='lightblue', dash='dot')))
        fig_temp.add_trace(go.Scatter(x=df_daily['log_date'], y=df_daily['supply_avg_c'].astype(float),
                                       name='Supply Avg', line=dict(color='red')))
        fig_temp.add_trace(go.Scatter(x=df_daily['log_date'], y=df_daily['return_avg_c'].astype(float),
                                       name='Return Avg', line=dict(color='orange')))
        fig_temp.update_layout(title='Daily Temperatures', height=350, margin=dict(t=40, b=20))
        st.plotly_chart(fig_temp, use_container_width=True)

        # ΔT and DM chart
        col_dt, col_dm = st.columns(2)
        with col_dt:
            fig_dt = go.Figure()
            fig_dt.add_trace(go.Bar(x=df_daily['log_date'], y=df_daily['delta_t_avg_k'].astype(float),
                                     name='ΔT Avg', marker_color='teal'))
            fig_dt.add_hline(y=5, line_dash='dash', line_color='orange', annotation_text='Target max 5K')
            fig_dt.add_hline(y=7, line_dash='dash', line_color='red', annotation_text='Critical 7K')
            fig_dt.update_layout(title='Delta T (Flow Quality)', height=300, margin=dict(t=40, b=20))
            st.plotly_chart(fig_dt, use_container_width=True)

        with col_dm:
            fig_dm = go.Figure()
            fig_dm.add_trace(go.Scatter(x=df_daily['log_date'], y=df_daily['dm_avg'].astype(float),
                                         name='DM Avg', line=dict(color='darkblue')))
            fig_dm.add_trace(go.Scatter(x=df_daily['log_date'], y=df_daily['dm_min'].astype(float),
                                         name='DM Min', line=dict(color='red', dash='dot')))
            fig_dm.add_hline(y=-200, line_dash='dash', line_color='orange', annotation_text='Warning -200')
            fig_dm.add_hline(y=-400, line_dash='dash', line_color='red', annotation_text='Critical -400')
            fig_dm.update_layout(title='Degree Minutes (Demand)', height=300, margin=dict(t=40, b=20))
            st.plotly_chart(fig_dm, use_container_width=True)

        # Heater and Superheat
        col_h, col_s = st.columns(2)
        with col_h:
            fig_h = go.Figure()
            fig_h.add_trace(go.Bar(x=df_daily['log_date'], y=df_daily['heater_energy_kwh'].astype(float),
                                    name='Heater kWh', marker_color='red'))
            fig_h.update_layout(title='Heater Energy (kWh/day)', height=300, margin=dict(t=40, b=20))
            st.plotly_chart(fig_h, use_container_width=True)

        with col_s:
            fig_s = go.Figure()
            fig_s.add_trace(go.Scatter(x=df_daily['log_date'], y=df_daily['superheat_avg_k'].astype(float),
                                        name='Superheat Avg', line=dict(color='teal')))
            fig_s.add_trace(go.Bar(x=df_daily['log_date'], y=df_daily['neg_superheat_pct'].astype(float),
                                    name='Neg Superheat %', marker_color='red', opacity=0.4, yaxis='y2'))
            fig_s.add_hline(y=0, line_dash='dash', line_color='red')
            fig_s.update_layout(
                title='Superheat (Compressor Health)', height=300, margin=dict(t=40, b=20),
                yaxis2=dict(overlaying='y', side='right', title='Neg %')
            )
            st.plotly_chart(fig_s, use_container_width=True)

        # Flow quality by freq band
        st.subheader("Flow Quality by Compressor Load")
        df_flow = run_query(f"""
            SELECT freq_band,
                   ROUND(AVG(delta_t_avg_k), 1) AS delta_t_avg_k,
                   ROUND(AVG(heater_pct), 0) AS heater_pct,
                   SUM(samples) AS samples
            FROM {_GL}.gold_nibe_flow_quality
            WHERE log_date >= DATE_SUB(CURRENT_DATE(), {days_back})
            GROUP BY freq_band ORDER BY freq_band
        """)
        if len(df_flow) == 0:
            df_flow = run_query(f"""
                SELECT freq_band,
                       ROUND(AVG(delta_t_avg_k), 1) AS delta_t_avg_k,
                       ROUND(AVG(heater_pct), 0) AS heater_pct,
                       SUM(samples) AS samples
                FROM {_GL}.gold_nibe_flow_quality
                GROUP BY freq_band ORDER BY freq_band
            """)
        if len(df_flow) > 0:
            fig_flow = go.Figure()
            fig_flow.add_trace(go.Bar(x=df_flow['freq_band'], y=df_flow['delta_t_avg_k'].astype(float),
                                       name='ΔT (K)', marker_color='teal'))
            fig_flow.add_trace(go.Bar(x=df_flow['freq_band'], y=df_flow['heater_pct'].astype(float),
                                       name='Heater %', marker_color='red', yaxis='y2'))
            fig_flow.add_hline(y=5, line_dash='dash', line_color='orange')
            fig_flow.update_layout(
                height=300, margin=dict(t=30, b=20), barmode='group',
                yaxis2=dict(overlaying='y', side='right', title='Heater %')
            )
            st.plotly_chart(fig_flow, use_container_width=True)

# =====================================================
# TAB 2: AGENT DIAGNOSIS
# =====================================================
with tab2:
    st.subheader("AI-Powered Diagnosis")
    st.caption("Pre-fetches diagnostic tool data, then calls an LLM endpoint for natural-language analysis.")

    _agent_endpoint = os.getenv("NIBE_AGENT_ENDPOINT", "")  # e.g. nibe-agent or databricks-claude-sonnet-4

    col_look, col_q = st.columns([1, 5])
    with col_look:
        agent_days = st.selectbox("Lookback", [3, 7, 14, 30, 60, 90], index=1, key="agent_days")
    with col_q:
        user_question = st.text_input(
            "Ask about your heat pump",
            placeholder="How is the pump performing? / What caused yesterday's alarm? / Compare this week vs last week",
        )

    if st.button("Ask Agent", type="primary") and user_question:
        with st.spinner("Fetching diagnostic data..."):
            tool_data = {}
            for tool_name in ['get_system_health', 'get_daily_summary',
                              'get_flow_diagnosis', 'get_superheat_status']:
                try:
                    df_t = run_query(f"SELECT {_GL}.{tool_name}({agent_days}) AS result")
                    tool_data[tool_name] = df_t.iloc[0, 0]
                except Exception as e:
                    tool_data[tool_name] = f"Error: {e}"

        if _agent_endpoint:
            import requests
            with st.spinner("Agent is analysing..."):
                context = "\n".join([f"{k}: {v}" for k, v in tool_data.items()])
                try:
                    resp = requests.post(
                        f"{_cfg.host}/serving-endpoints/{_agent_endpoint}/invocations",
                        headers={"Authorization": f"Bearer {_cfg.token}"},
                        json={
                            "messages": [
                                {"role": "user", "content": (
                                    f"User question: {user_question}\n\n"
                                    f"Diagnostic tool data (last {agent_days} days):\n{context}"
                                )}
                            ]
                        },
                        timeout=60,
                    )
                    if resp.ok:
                        answer = resp.json()["choices"][0]["message"]["content"]
                        st.markdown(answer)
                    else:
                        st.error(f"Agent endpoint error {resp.status_code}: {resp.text[:300]}")
                except Exception as e:
                    st.error(f"Request failed: {e}")
        else:
            st.info(
                "No LLM endpoint configured. Set `NIBE_AGENT_ENDPOINT` in app.yaml env to enable. "
                "Raw tool data is shown below."
            )

        with st.expander("Raw Tool Data", expanded=not _agent_endpoint):
            for name, data in tool_data.items():
                st.write(f"**{name}:**")
                try:
                    parsed = json.loads(data)
                    if isinstance(parsed, list):
                        st.dataframe(pd.DataFrame(parsed), use_container_width=True)
                    elif isinstance(parsed, dict):
                        st.json(parsed)
                    else:
                        st.json(parsed)
                except Exception:
                    st.code(str(data)[:500])

    # Period comparison
    st.divider()
    st.subheader("Period Comparison")
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        st.write("**Period 1** (e.g. Jan 2026 — before fix)")
        p1_start = st.date_input("Start", value=pd.Timestamp("2026-01-12"), key="p1s")
        p1_end = st.date_input("End", value=pd.Timestamp("2026-01-18"), key="p1e")
    with col_p2:
        st.write("**Period 2** (e.g. Feb 2026 — after fix)")
        p2_start = st.date_input("Start", value=pd.Timestamp("2026-02-15"), key="p2s")
        p2_end = st.date_input("End", value=pd.Timestamp("2026-02-16"), key="p2e")

    if st.button("Compare Periods"):
        with st.spinner("Comparing..."):
            try:
                df_comp = run_query(f"""
                    SELECT {_GL}.get_period_comparison(
                        '{p1_start}', '{p1_end}', '{p2_start}', '{p2_end}'
                    ) AS result
                """)
                parsed = json.loads(df_comp.iloc[0, 0])
                st.dataframe(pd.DataFrame(parsed).T, use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")

# =====================================================
# TAB 3: RAW LOG VIEWER (LogViewer parity)
# =====================================================
with tab3:
    st.subheader("Raw Log Viewer")
    st.caption("Interactive parameter viewer — select date, hour, and parameters to plot.")

    col_date, col_hour = st.columns(2)
    with col_date:
        target_date = st.date_input("Date", value=pd.Timestamp("2026-01-17"), key="raw_date")
    with col_hour:
        target_hour = st.slider("Hour", 0, 23, 9)

    all_params = [
        "bt1_outdoor_temp_c", "bt25_supply_c", "bt71_return_c",
        "calc_supply_c", "compr_freq_act_hz", "degree_minutes",
        "bt16_evap_c", "bt17_suction_c", "bt14_discharge_c",
        "bp4_hp_bar", "lp_raw", "tot_int_add_kw",
        "gp12_fan_pct", "bt6_dhw_top_c", "bt28_outdoor_module_c",
        "delta_t_k", "superheat_k",
    ]

    selected = st.multiselect(
        "Parameters to plot",
        all_params,
        default=["bt25_supply_c", "bt71_return_c", "compr_freq_act_hz"],
    )

    if selected:
        cols_str = ", ".join(selected)
        _SL = f"{_env}_silver.nibe"  # silver layer for raw logs
        raw_df = run_query(f"""
            SELECT logged_at, {cols_str}
            FROM {_SL}.silver_nibe_logs
            WHERE log_date_parsed = '{target_date}'
              AND HOUR(logged_at) = {target_hour}
            ORDER BY logged_at
        """)

        if len(raw_df) == 0:
            st.warning("No data for selected date/hour.")
        else:
            st.write(f"**{len(raw_df)} samples** ({target_date} {target_hour}:00–{target_hour}:59)")

            # Cast selected columns to float for plotting
            plot_df = raw_df.copy()
            for col in selected:
                plot_df[col] = pd.to_numeric(plot_df[col], errors='coerce')

            fig_raw = px.line(plot_df, x='logged_at', y=selected,
                              title=f"Raw Logs: {target_date} {target_hour:02d}:00")
            fig_raw.update_layout(height=500, showlegend=True, margin=dict(t=40, b=20))
            st.plotly_chart(fig_raw, use_container_width=True)

            if st.checkbox("Show full day (1-minute averages)"):
                cols_avg = ", ".join([f"ROUND(AVG(CAST({p} AS DOUBLE)), 2) AS {p}" for p in selected])
                full_day = run_query(f"""
                    SELECT DATE_TRUNC('MINUTE', logged_at) AS ts,
                           {cols_avg}
                    FROM {_SL}.silver_nibe_logs
                    WHERE log_date_parsed = '{target_date}'
                    GROUP BY DATE_TRUNC('MINUTE', logged_at)
                    ORDER BY ts
                """)
                full_day_plot = full_day.copy()
                for col in selected:
                    full_day_plot[col] = pd.to_numeric(full_day_plot[col], errors='coerce')
                fig_full = px.line(full_day_plot, x='ts', y=selected,
                                    title=f"Full Day: {target_date} (1-min avg)")
                fig_full.update_layout(height=500)
                st.plotly_chart(fig_full, use_container_width=True)

            col_csv, _ = st.columns([1, 5])
            with col_csv:
                csv = raw_df.to_csv(index=False)
                st.download_button(
                    "Export CSV", csv,
                    f"nibe_raw_{target_date}_{target_hour:02d}.csv",
                    mime="text/csv",
                )

# --- Sidebar ---
st.sidebar.markdown("### System Info")
st.sidebar.markdown("NIBE AMS 10/12 + BA-SVM 10-200")
st.sidebar.markdown("Underfloor Heating, Ciechanów PL")
st.sidebar.divider()
try:
    dq = run_query(f"""
        SELECT * FROM {_GL}.gold_nibe_data_quality ORDER BY log_date DESC LIMIT 1
    """)
    if len(dq) > 0:
        st.sidebar.metric("Latest Data", str(dq.iloc[0]['log_date']))
        st.sidebar.metric("Coverage", f"{float(dq.iloc[0]['coverage_pct']):.1f}%")
except Exception:
    pass
