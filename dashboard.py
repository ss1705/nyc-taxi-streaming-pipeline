import streamlit as st
import duckdb
import pandas as pd
import altair as alt

# --- Config ---
st.set_page_config(
    page_title="NYC Taxi Demand Dashboard",
    page_icon="🚕",
    layout="wide"
)

# --- Load data ---
@st.cache_data
def load_data():
    conn = duckdb.connect("warehouse.duckdb")
    df = conn.execute("SELECT * FROM mart_hourly_demand").df()
    conn.close()
    return df

df = load_data().copy()

# --- Header ---
st.title("🚕 NYC Taxi Demand Dashboard")
st.caption("Real-time pipeline: Kafka → PySpark → DuckDB → dbt → Streamlit")

# --- KPI row ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trips",       f"{df['trip_count'].sum():,}")
col2.metric("Avg Fare",          f"${df['avg_fare'].mean():.2f}")
col3.metric("Avg Trip Duration", f"{df['avg_duration_min'].mean():.1f} min")
col4.metric("Avg Tip %",         f"{df['avg_tip_pct'].mean():.1f}%")

st.divider()

# --- Chart 1: Hourly demand ---
st.subheader("Trip Demand by Hour of Day")
st.caption("Trips aggregated across all days — reveals natural demand rhythm throughout the day.")

hourly = df.groupby("pickup_hour", as_index=False)["trip_count"].sum()

chart1 = alt.Chart(hourly).mark_bar(
    color="#4C9BE8",
    cornerRadiusTopLeft=3,
    cornerRadiusTopRight=3
).encode(
    x=alt.X("pickup_hour:O",
            title="Hour of Day (0 = midnight, 12 = noon)",
            axis=alt.Axis(labelAngle=0)),
    y=alt.Y("trip_count:Q",
            title="Number of Trips",
            scale=alt.Scale(zero=True)),
    tooltip=[
        alt.Tooltip("pickup_hour:O", title="Hour"),
        alt.Tooltip("trip_count:Q", title="Trips")
    ]
).properties(height=350)

st.altair_chart(chart1, use_container_width=True)

st.divider()

# --- Chart 2: Demand by time of day ---
st.subheader("Trip Demand by Time of Day")
st.caption("Midday and night periods dominate demand — useful for fleet allocation decisions.")

time_order = ["overnight", "morning_rush", "midday", "evening_rush", "night"]
tod = df.groupby("time_of_day", as_index=False)["trip_count"].sum()
tod["time_of_day"] = pd.Categorical(
    tod["time_of_day"], categories=time_order, ordered=True
)
tod = tod.sort_values("time_of_day")

chart2 = alt.Chart(tod).mark_bar(
    cornerRadiusTopLeft=3,
    cornerRadiusTopRight=3
).encode(
    x=alt.X("time_of_day:O",
            title="Time of Day",
            sort=time_order,
            axis=alt.Axis(labelAngle=0)),
    y=alt.Y("trip_count:Q",
            title="Number of Trips",
            scale=alt.Scale(zero=True)),
    color=alt.Color("time_of_day:O",
                    scale=alt.Scale(scheme="blues"),
                    legend=None),
    tooltip=[
        alt.Tooltip("time_of_day:O", title="Period"),
        alt.Tooltip("trip_count:Q", title="Trips")
    ]
).properties(height=350)

st.altair_chart(chart2, use_container_width=True)

st.divider()

# --- Chart 3: Avg fare by time of day ---
st.subheader("Average Fare by Time of Day")
st.caption("Overnight trips command higher fares — consistent with longer airport and cross-borough rides.")

tod_fare = df.groupby("time_of_day", as_index=False).agg(
    avg_fare=("avg_fare", "mean"),
    avg_duration=("avg_duration_min", "mean")
)
tod_fare["time_of_day"] = pd.Categorical(
    tod_fare["time_of_day"], categories=time_order, ordered=True
)
tod_fare = tod_fare.sort_values("time_of_day")

chart3 = alt.Chart(tod_fare).mark_bar(
    cornerRadiusTopLeft=3,
    cornerRadiusTopRight=3
).encode(
    x=alt.X("time_of_day:O",
            title="Time of Day",
            sort=time_order,
            axis=alt.Axis(labelAngle=0)),
    y=alt.Y("avg_fare:Q",
            title="Average Fare ($)",
            scale=alt.Scale(zero=True)),
    color=alt.Color("avg_fare:Q",
                    scale=alt.Scale(scheme="tealblues"),
                    legend=None),
    tooltip=[
        alt.Tooltip("time_of_day:O", title="Period"),
        alt.Tooltip("avg_fare:Q", title="Avg Fare ($)", format=".2f"),
        alt.Tooltip("avg_duration:Q", title="Avg Duration (min)", format=".1f")
    ]
).properties(height=350)

st.altair_chart(chart3, use_container_width=True)

st.divider()

# --- Table ---
st.subheader("Demand by Hour and Day of Week")
day_labels = {1:"Sun", 2:"Mon", 3:"Tue", 4:"Wed", 5:"Thu", 6:"Fri", 7:"Sat"}
table_df = df.copy()
table_df["day_label"] = table_df["pickup_dayofweek"].map(day_labels)
st.dataframe(
    table_df.sort_values(["pickup_dayofweek", "pickup_hour"])
    [["day_label", "pickup_hour", "time_of_day",
      "trip_count", "avg_fare", "avg_duration_min", "avg_tip_pct"]]
    .reset_index(drop=True),
    use_container_width=True
)

st.divider()
st.caption("Data: NYC TLC 2014–2015 | Pipeline: Kafka (KRaft) → PySpark 3.5.1 → DuckDB → dbt 1.11 → Streamlit")