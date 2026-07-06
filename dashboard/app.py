
# PURPOSE — Single Streamlit file that connects to DuckDB, reads all gold tables, and displays interactive charts and metrics.



import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


st.set_page_config(
    page_title="India AQI Dashboard",
    page_icon="🌫️",
    layout="wide"
)


@st.cache_resource
def get_connection():
    conn = duckdb.connect("gold/aqi_warehouse.duckdb")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    return conn

conn = get_connection()




st.title("🌫️ India AQI Real-Time Dashboard")
st.markdown("**Live Air Quality Index monitoring for 10 major Indian cities**")
st.divider()

# load data
df_risk = conn.execute("SELECT * FROM city_health_risk_score;").df()
df_top  = conn.execute("SELECT * FROM top_polluted_cities;").df()
df_cat  = conn.execute("SELECT * FROM aqi_category_distribution;").df()
df_poll = conn.execute("SELECT * FROM most_dangerous_pollutant;").df()
df_sum  = conn.execute("SELECT * FROM city_aqi_summary;").df()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Most Polluted City",
              df_top.iloc[0]["city"],
              f"AQI {df_top.iloc[0]['avg_aqi']}")
with col2:
    st.metric("Cleanest City",
              df_risk.iloc[-1]["city"],
              f"Risk Score {df_risk.iloc[-1]['health_risk_score']}")
with col3:
    st.metric("Cities at Risk",
              f"{len(df_risk[df_risk['risk_level'].isin(['HIGH','CRITICAL'])])} / 10",
              "HIGH or CRITICAL")
with col4:
    st.metric("Dominant Pollutant",
              df_poll['dominant_pollutant'].mode()[0],
              "across all cities")

st.divider()




st.subheader("🏭 Top Polluted Cities by Average AQI")

col1, col2 = st.columns([2, 1])

with col1:
    fig_bar = px.bar(
        df_top,
        x="city",
        y="avg_aqi",
        color="aqi_category",
        text="avg_aqi",
        title="Top 5 Most Polluted Cities",
        color_discrete_map={
            "Poor": "#EF4444",
            "Moderate": "#F59E0B",
            "Satisfactory": "#22C55E",
            "Good": "#3B82F6"
        }
    )
    fig_bar.update_traces(textposition="outside")
    fig_bar.update_layout(showlegend=True, height=400)
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    st.dataframe(df_top[["city", "avg_aqi", "aqi_category"]],
                 use_container_width=True, hide_index=True)

st.divider()





st.subheader("⚠️ City Health Risk Score")

def color_risk(val):
    colors = {
        "CRITICAL": "background-color: #FEE2E2; color: #B91C1C; font-weight: bold",
        "HIGH":     "background-color: #FEF3C7; color: #92400E; font-weight: bold",
        "MODERATE": "background-color: #FEF9C3; color: #713F12",
        "LOW":      "background-color: #DCFCE7; color: #15803D"
    }
    return colors.get(val, "")

styled_df = df_risk[["city", "state", "avg_aqi", "avg_pm25",
                      "avg_no2", "health_risk_score", "risk_level"]] \
    .style.applymap(color_risk, subset=["risk_level"])

st.dataframe(styled_df, use_container_width=True, hide_index=True)
st.divider()





st.subheader("☣️ Most Dangerous Pollutant by City")

col1, col2 = st.columns([2, 1])

with col1:
    fig_poll = px.bar(
        df_poll,
        x="city",
        y=["avg_pm25", "avg_pm10", "avg_no2", "avg_so2"],
        title="Pollutant Levels by City (μg/m³)",
        barmode="group",
        color_discrete_map={
            "avg_pm25": "#EF4444",
            "avg_pm10": "#F59E0B",
            "avg_no2":  "#8B5CF6",
            "avg_so2":  "#06B6D4"
        }
    )
    fig_poll.update_layout(height=400, legend_title="Pollutant")
    st.plotly_chart(fig_poll, use_container_width=True)

with col2:
    st.dataframe(
        df_poll[["city", "dominant_pollutant"]],
        use_container_width=True,
        hide_index=True
    )

st.divider()






st.subheader("🥧 National AQI Category Distribution")

col1, col2 = st.columns([1, 2])

with col1:
    fig_pie = px.pie(
        df_cat,
        names="aqi_category",
        values="city_count",
        title="Cities by AQI Category",
        color="aqi_category",
        color_discrete_map={
            "Good":         "#3B82F6",
            "Satisfactory": "#22C55E",
            "Moderate":     "#F59E0B",
            "Poor":         "#EF4444",
            "Very Poor":    "#7C3AED",
            "Hazardous":    "#1E293B"
        }
    )
    fig_pie.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.dataframe(df_cat, use_container_width=True, hide_index=True)
    st.markdown("---")
    st.markdown("**Data Source:** OpenWeatherMap Air Pollution API")
    st.markdown("**Cities:** Delhi, Mumbai, Kolkata, Chennai, Bengaluru, "
                "Hyderabad, Patna, Lucknow, Ahmedabad, Jaipur")
    st.markdown("**Pipeline:** Kafka → Bronze → Silver → Gold → Dashboard")
    st.markdown("**Built by:** Arish ")





st.divider()
st.subheader("🤖 LSTM AQI Predictions vs Actual")

try:
    import mlflow
    import mlflow.pytorch
    import torch
    from ml.data_loader import load_aqi_data
    from ml.preprocessor import scale_data, create_sequences
    from ml.model import predict
    import numpy as np

    mlflow.set_tracking_uri("mlruns")
    df_ml = load_aqi_data()
    scaled_df, scaler = scale_data(df_ml)
    data = scaled_df.values
    X, y = create_sequences(data, sequence_length=3)

    if len(X) >= 5:
        split = int(len(X) * 0.8)
        X_test = X[split:]
        y_test = y[split:]
        runs = mlflow.search_runs(
            experiment_names=["AQI_Forecasting"],
            order_by=["metrics.mae ASC"]
        )
        if len(runs) > 0:
            best_run_id = runs.iloc[0]["run_id"]
            model = mlflow.pytorch.load_model(f"runs:/{best_run_id}/aqi_lstm_model")
            predictions = predict(model, X_test, scaler)
            actual = scaler.inverse_transform(
                np.column_stack([y_test, np.zeros((len(y_test), 2))])
            )[:, 0]
            df_pred = pd.DataFrame({
                "Sample": [f"Sample {i+1}" for i in range(len(predictions))],
                "Predicted AQI": predictions.round(1),
                "Actual AQI": actual.round(1),
                "Error": abs(predictions - actual).round(1)
            })
            col1, col2 = st.columns([2, 1])
            with col1:
                fig_pred = px.bar(
                    df_pred,
                    x="Sample",
                    y=["Predicted AQI", "Actual AQI"],
                    barmode="group",
                    title="LSTM Predicted vs Actual AQI",
                    color_discrete_map={
                        "Predicted AQI": "#8B5CF6",
                        "Actual AQI": "#06B6D4"
                    }
                )
                st.plotly_chart(fig_pred, use_container_width=True)
            with col2:
                st.dataframe(df_pred, use_container_width=True, hide_index=True)
                mae = df_pred["Error"].mean()
                st.metric("Mean Absolute Error", f"{mae:.1f} AQI units")
        else:
            st.warning("No MLflow runs found. Run python -m ml.main first.")
    else:
        st.warning("Not enough data for predictions.")
except Exception as e:
    st.error(f"ML predictions unavailable: {e}")