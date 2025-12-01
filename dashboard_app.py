import os
import glob
import pandas as pd
from dash import Dash, dcc, html
import plotly.express as px

BASE_RESULTS_DIR = "results"

def load_single_csv(subdir: str) -> pd.DataFrame:
    pattern = os.path.join(BASE_RESULTS_DIR, subdir, "part-*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No CSV files found for pattern: {pattern}")
    return pd.read_csv(files[0])

# ---- Load data ----
df_distance = load_single_csv("avg_trip_distance_by_hour")      # pickup_hour, avg_trip_distance
df_tip = load_single_csv("daily_tip_percent")                   # pickup_date, avg_tip_percent
df_fare = load_single_csv("fare_by_hour")                       # pickup_hour, avg_fare
df_passengers = load_single_csv("passenger_count_distribution") # passenger_count, trip_count, percent
df_rev_per_mile = load_single_csv("revenue_per_mile")           # pickup_hour, avg_revenue_per_mile
df_duration = load_single_csv("trip_duration_stats")            # min/mean/median/max_trip_duration_minutes

# ---- Type cleanup ----
# Daily tip: date
df_tip["pickup_date"] = pd.to_datetime(df_tip["pickup_date"])

# Hour-based metrics: make sure they’re ints so plots sort nicely 0–23
for df in [df_distance, df_fare, df_rev_per_mile]:
    if "pickup_hour" in df.columns:
        df["pickup_hour"] = df["pickup_hour"].astype(int)

# Passenger count: ensure int
df_passengers["passenger_count"] = df_passengers["passenger_count"].astype(int)

# ---- Build figures ----

# 1) Average trip distance by pickup hour
fig_distance = px.bar(
    df_distance,
    x="pickup_hour",
    y="avg_trip_distance",
    title="Average Trip Distance by Pickup Hour",
    labels={"pickup_hour": "Hour of Day", "avg_trip_distance": "Avg Distance (miles)"}
)

# 2) Average fare by pickup hour
fig_fare = px.line(
    df_fare,
    x="pickup_hour",
    y="avg_fare",
    title="Average Fare by Pickup Hour",
    markers=True,
    labels={"pickup_hour": "Hour of Day", "avg_fare": "Avg Fare ($)"}
)

# 3) Daily tip percentage over time
fig_tip = px.line(
    df_tip.sort_values("pickup_date"),
    x="pickup_date",
    y="avg_tip_percent",
    title="Average Tip Percentage by Day",
    markers=True,
    labels={"pickup_date": "Date", "avg_tip_percent": "Avg Tip (%)"}
)

# 4) Passenger count distribution
fig_passengers = px.bar(
    df_passengers.sort_values("passenger_count"),
    x="passenger_count",
    y="trip_count",
    title="Passenger Count Distribution",
    labels={"passenger_count": "Passenger Count", "trip_count": "Number of Trips"}
)

# 5) Revenue per mile by pickup hour
fig_rev_per_mile = px.line(
    df_rev_per_mile,
    x="pickup_hour",
    y="avg_revenue_per_mile",
    title="Average Revenue per Mile by Pickup Hour",
    markers=True,
    labels={
        "pickup_hour": "Hour of Day",
        "avg_revenue_per_mile": "Avg Revenue per Mile ($/mile)",
    },
)

# 6) Trip duration stats (single-row stats)
df_duration_melt = df_duration.melt(
    var_name="metric",
    value_name="minutes"
)

fig_duration = px.bar(
    df_duration_melt,
    x="metric",
    y="minutes",
    title="Trip Duration Statistics (Minutes)",
    labels={"metric": "Statistic", "minutes": "Minutes"}
)

# ---- Dash App ----
app = Dash(__name__)

app.layout = html.Div(
    style={
        "fontFamily": "Arial",
        "padding": "20px",
        "maxWidth": "1400px",
        "margin": "0 auto",
        "backgroundColor": "#f4f4f4",
    },
    children=[
        html.H1(
            "NYC Taxi Trip Data Dashboard",
            style={"textAlign": "center", "marginBottom": "30px"},
        ),

        html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "1fr 1fr",
                "gap": "20px",
            },
            children=[
                html.Div(
                    children=[
                        html.H3("Trip Distance by Hour"),
                        dcc.Graph(
                            id="trip-distance-by-hour",
                            figure=fig_distance,
                            style={"height": "400px"},
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.H3("Fare by Hour"),
                        dcc.Graph(
                            id="fare-by-hour",
                            figure=fig_fare,
                            style={"height": "400px"},
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.H3("Daily Tip Percentage"),
                        dcc.Graph(
                            id="daily-tip-percentage",
                            figure=fig_tip,
                            style={"height": "400px"},
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.H3("Passenger Count Distribution"),
                        dcc.Graph(
                            id="passenger-count-distribution",
                            figure=fig_passengers,
                            style={"height": "400px"},
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.H3("Revenue per Mile by Hour"),
                        dcc.Graph(
                            id="revenue-per-mile-by-hour",
                            figure=fig_rev_per_mile,
                            style={"height": "400px"},
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.H3("Trip Duration Statistics"),
                        dcc.Graph(
                            id="trip-duration-statistics",
                            figure=fig_duration,
                            style={"height": "400px"},
                        ),
                    ]
                ),
            ],
        ),
    ],
)

if __name__ == "__main__":
    # host='0.0.0.0' so you can reach it if needed from outside the VM
    app.run(debug=True, host="0.0.0.0", port=8050)
