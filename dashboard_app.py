import os
import glob
import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

BASE_RESULTS_DIR = "results"

def load_single_csv(subdir: str) -> pd.DataFrame:
    pattern = os.path.join(BASE_RESULTS_DIR, subdir, "part-*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No CSV files found for pattern: {pattern}")
    return pd.read_csv(files[0])

# =========================
# LOAD ANALYTICS DATA
# =========================
df_distance = load_single_csv("avg_trip_distance_by_hour")
df_tip = load_single_csv("daily_tip_percent")
df_fare = load_single_csv("fare_by_hour")
df_passengers = load_single_csv("passenger_count_distribution")
df_rev_per_mile = load_single_csv("revenue_per_mile")
df_duration = load_single_csv("trip_duration_stats")

df_tip["pickup_date"] = pd.to_datetime(df_tip["pickup_date"])

for df in [df_distance, df_fare, df_rev_per_mile]:
    if "pickup_hour" in df.columns:
        df["pickup_hour"] = df["pickup_hour"].astype(int)

df_passengers["passenger_count"] = df_passengers["passenger_count"].astype(int)

# =========================
# LOAD BENCHMARK DATA
# =========================
df_cache = pd.read_csv("results/benchmarks/cache_effects/cache_benchmark.csv")
df_format = pd.read_csv("results/benchmarks/csv_vs_parquet/csv_vs_parquet_benchmark.csv")
df_shuffle = pd.read_csv("results/benchmarks/shuffle_partitions/shuffle_partitions_benchmark.csv")

# =========================
# BUILD ANALYTICS FIGURES
# =========================
fig_distance = px.bar(df_distance, x="pickup_hour", y="avg_trip_distance",
    title="Average Trip Distance by Pickup Hour")

fig_fare = px.line(df_fare, x="pickup_hour", y="avg_fare", markers=True,
    title="Average Fare by Pickup Hour")

fig_tip = px.line(df_tip.sort_values("pickup_date"), x="pickup_date",
    y="avg_tip_percent", markers=True, title="Average Tip Percentage by Day")

fig_passengers = px.bar(df_passengers.sort_values("passenger_count"),
    x="passenger_count", y="trip_count", title="Passenger Count Distribution")

fig_rev_per_mile = px.line(df_rev_per_mile, x="pickup_hour",
    y="avg_revenue_per_mile", markers=True,
    title="Average Revenue per Mile by Pickup Hour")

df_duration_melt = df_duration.melt(var_name="metric", value_name="minutes")

fig_duration = px.bar(df_duration_melt, x="metric", y="minutes",
    title="Trip Duration Statistics (Minutes)")

# =========================
# BUILD BENCHMARK FIGURES
# =========================
fig_cache = px.bar(
    df_cache,
    x="method",
    y="seconds",
    title="Cache Effects Benchmark",
    text="seconds"
)

fig_format = px.bar(
    df_format,
    x="format",
    y="seconds",
    title="CSV vs Parquet Benchmark",
    text="seconds"
)

fig_shuffle = px.line(
    df_shuffle,
    x="shuffle_partitions",
    y="seconds",
    title="Shuffle Partitions Benchmark",
    markers=True
)

# =========================
# DASH APP
# =========================
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

        html.H1("NYC Taxi Trip Data Dashboard",
            style={"textAlign": "center", "marginBottom": "30px"}),

        dcc.Tabs([

            # ==========================
            # ANALYTICS TAB
            # ==========================
            dcc.Tab(label="Analytics", children=[
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "20px"},
                    children=[
                        html.Div([html.H3("Trip Distance by Hour"),
                            dcc.Graph(figure=fig_distance, style={"height": "400px"})]),
                        html.Div([html.H3("Fare by Hour"),
                            dcc.Graph(figure=fig_fare, style={"height": "400px"})]),
                        html.Div([html.H3("Daily Tip Percentage"),
                            dcc.Graph(figure=fig_tip, style={"height": "400px"})]),
                        html.Div([html.H3("Passenger Count Distribution"),
                            dcc.Graph(figure=fig_passengers, style={"height": "400px"})]),
                        html.Div([html.H3("Revenue per Mile by Hour"),
                            dcc.Graph(figure=fig_rev_per_mile, style={"height": "400px"})]),
                        html.Div([html.H3("Trip Duration Statistics"),
                            dcc.Graph(figure=fig_duration, style={"height": "400px"})]),
                    ],
                )
            ]),

            # ==========================
            # BENCHMARKS TAB
            # ==========================
            dcc.Tab(label="Benchmarks", children=[

                html.H2("Performance Benchmarks", style={"textAlign": "center"}),

                html.Div([
                    html.H3("Cache Effects"),
                    dcc.Graph(figure=fig_cache, style={"height": "350px"}),
                    html.P("Shows the dramatic performance improvement when Spark caching is used."),
                    html.Button("Download CSV", id="btn-cache"),
                ]),

                html.Div([
                    html.H3("CSV vs Parquet"),
                    dcc.Graph(figure=fig_format, style={"height": "350px"}),
                    html.P("Demonstrates how Parquet's columnar binary format vastly outperforms CSV."),
                    html.Button("Download CSV", id="btn-format"),
                ]),

                html.Div([
                    html.H3("Shuffle Partition Tuning"),
                    dcc.Graph(figure=fig_shuffle, style={"height": "350px"}),
                    html.P("Shows how tuning shuffle partitions impacts performance."),
                    html.Button("Download CSV", id="btn-shuffle"),
                ]),

                dcc.Download(id="download-cache"),
                dcc.Download(id="download-format"),
                dcc.Download(id="download-shuffle"),
            ]),

        ]),
    ],
)

# =========================
# DOWNLOAD CALLBACKS
# =========================

@app.callback(Output("download-cache", "data"),
    Input("btn-cache", "n_clicks"), prevent_initial_call=True)
def download_cache(_):
    return dcc.send_data_frame(df_cache.to_csv, "cache_benchmark.csv")

@app.callback(Output("download-format", "data"),
    Input("btn-format", "n_clicks"), prevent_initial_call=True)
def download_format(_):
    return dcc.send_data_frame(df_format.to_csv, "csv_vs_parquet.csv")

@app.callback(Output("download-shuffle", "data"),
    Input("btn-shuffle", "n_clicks"), prevent_initial_call=True)
def download_shuffle(_):
    return dcc.send_data_frame(df_shuffle.to_csv, "shuffle_partitions.csv")

# =========================
# RUN SERVER
# =========================
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
