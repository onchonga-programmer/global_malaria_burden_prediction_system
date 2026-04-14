import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8001")
REGIONS = ["AFRO", "AMRO", "EMRO", "SEARO", "WPRO"]

st.set_page_config(
    page_title="Global Malaria Burden Dashboard",
    page_icon="🦟",
    layout="wide"
)

# ── API calls 
@st.cache_data(ttl=300)  # cache for 5 minutes
def get_forecast(region: str):
    try:
        r = requests.get(f"{FASTAPI_URL}/forecast", params={"region": region}, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Failed to fetch forecast: {e}")
        return None

@st.cache_data(ttl=300)
def get_classification(region: str):
    try:
        r = requests.get(f"{FASTAPI_URL}/classify", params={"region": region}, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Failed to fetch classification: {e}")
        return None

@st.cache_data(ttl=60)
def get_health():
    try:
        r = requests.get(f"{FASTAPI_URL}/health", timeout=5)
        return r.json()
    except Exception:
        return None

with st.sidebar:
    st.title("Controls")

    region = st.selectbox(
        "WHO Region",
        options=REGIONS,
        index=0,
        help="Select a WHO region to view malaria burden forecast"
    )

    st.divider()

    # API health indicator
    health = get_health()
    if health and health.get("status") == "ok":
        st.success("API connected")
        st.caption(f"{len(health['models_loaded'])} models loaded")
    else:
        st.error("API not reachable — is FastAPI running?")

    st.divider()
    st.caption("Data source: WHO / OWID")
    st.caption("Models: Prophet + LightGBM")
    st.caption("Explainability: SHAP")

# ── Main content ──────────────────────────────────────────────────
st.title("Global Malaria Burden Prediction System")
st.markdown(f"Showing forecasts and trend classification for **{region}**")

# Fetch data
forecast_data = get_forecast(region)
classify_data = get_classification(region)

if not forecast_data or not classify_data:
    st.warning("Could not load data. Check that FastAPI is running on port 8001.")
    st.stop()

# ── Row 1: Classification badge ───────────────────────────────────
col1, col2, col3 = st.columns(3)

prediction = classify_data["prediction"]
probability = classify_data["probability"]

with col1:
    if prediction == "improving":
        st.success(f"Trend: IMPROVING")
    else:
        st.error(f"Trend: DETERIORATING")
    st.caption(f"Model confidence: {probability:.0%}")

with col2:
    st.metric(
        label="Forecast 2025 (deaths)",
        value=f"{forecast_data['forecast'][0]['predicted_deaths']:,.0f}"
    )

with col3:
    st.metric(
        label="Forecast 2029 (deaths)",
        value=f"{forecast_data['forecast'][-1]['predicted_deaths']:,.0f}",
        delta=f"{forecast_data['forecast'][-1]['predicted_deaths'] - forecast_data['forecast'][0]['predicted_deaths']:+,.0f} vs 2025"
    )

st.divider()

st.subheader("5-Year Malaria Death Forecast")

forecast_df = pd.DataFrame(forecast_data["forecast"])

fig_forecast = go.Figure()

# Uncertainty band
fig_forecast.add_trace(go.Scatter(
    x=list(forecast_df["year"]) + list(forecast_df["year"])[::-1],
    y=list(forecast_df["upper_bound"]) + list(forecast_df["lower_bound"])[::-1],
    fill="toself",
    fillcolor="rgba(99, 110, 250, 0.15)",
    line=dict(color="rgba(255,255,255,0)"),
    name="95% confidence interval",
    hoverinfo="skip"
))

# Forecast line
fig_forecast.add_trace(go.Scatter(
    x=forecast_df["year"],
    y=forecast_df["predicted_deaths"],
    mode="lines+markers",
    name="Predicted deaths",
    line=dict(color="#636EFA", width=2.5),
    marker=dict(size=7)
))

fig_forecast.update_layout(
    xaxis=dict(tickmode="linear", tick0=2025, dtick=1),  # add this
    xaxis_title="Year",
    yaxis_title="Predicted deaths",
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
    margin=dict(l=0, r=0, t=30, b=0)
)

st.plotly_chart(fig_forecast, use_container_width=True)

st.divider()

st.subheader("What's Driving This Prediction?")
st.caption("SHAP values show how much each feature pushed the model toward 'improving' (positive) or 'deteriorating' (negative)")

shap_df = pd.DataFrame([
    {"feature": k, "shap_value": v}
    for k, v in classify_data["shap_values"].items()
]).sort_values("shap_value", ascending=True)

fig_shap = px.bar(
    shap_df,
    x="shap_value",
    y="feature",
    orientation="h",
    color="shap_value",
    color_continuous_scale=["#EF553B", "#FFFFFF", "#636EFA"],
    color_continuous_midpoint=0,
    labels={"shap_value": "SHAP value", "feature": "Feature"}
)

fig_shap.update_layout(
    coloraxis_showscale=False,
    margin=dict(l=0, r=0, t=10, b=0)
)

st.plotly_chart(fig_shap, use_container_width=True)

st.divider()

with st.expander("Show raw forecast data"):
    st.dataframe(forecast_df, use_container_width=True)
    st.caption(f"Model version: {forecast_data['model_version']}")