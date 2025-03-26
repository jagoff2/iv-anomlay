"""
Enhanced Options Pricing Anomaly Detection Dashboard (Fixed Version)

This version fixes the callback issues in the previous implementation.
"""

import os
import json
import time
import datetime
import numpy as np
import pandas as pd
import requests
import traceback
from typing import Dict, List, Optional, Tuple, Union

import dash
from dash import dcc, html, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Constants
ANOMALY_API_URL = os.environ.get('ANOMALY_API_URL', 'http://localhost:8000')
REFRESH_INTERVAL = 5000  # 5 seconds
MAX_ANOMALIES = 1000  # Maximum number of anomalies to display

# Initialize the Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}]
)
app.title = "Options Anomaly Detection"
server = app.server  # For WSGI deployment


# Functions to fetch data from API
def fetch_recent_anomalies() -> List[Dict]:
    """Fetch recent anomalies from the API with error handling"""
    try:
        print(f"Fetching anomalies from {ANOMALY_API_URL}/api/anomalies")
        response = requests.get(f"{ANOMALY_API_URL}/api/anomalies", timeout=5)
        
        print(f"API response status code: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            anomalies = result.get('anomalies', [])
            print(f"Fetched {len(anomalies)} anomalies from API")
            return anomalies
        else:
            print(f"Error fetching anomalies: {response.status_code}")
            return []
    except Exception as e:
        print(f"Exception fetching anomalies: {str(e)}")
        print(traceback.format_exc())
        return []


def fetch_stats() -> Dict:
    """Fetch system statistics from the API with error handling"""
    try:
        print(f"Fetching stats from {ANOMALY_API_URL}/api/stats")
        response = requests.get(f"{ANOMALY_API_URL}/api/stats", timeout=5)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching stats: {response.status_code}")
            return {}
    except Exception as e:
        print(f"Exception fetching stats: {str(e)}")
        return {}


def prepare_anomalies_dataframe(anomalies: List[Dict]) -> pd.DataFrame:
    """Convert anomalies list to DataFrame for visualization with additional trading information"""
    if not anomalies:
        return pd.DataFrame()
    
    # Extract option data from each anomaly
    records = []
    for anomaly in anomalies:
        option_data = anomaly.get('option_data', {})
        
        # Extract predicted IV from description if available
        description = anomaly.get('description', '')
        predicted_iv = None
        if 'Predicted IV:' in description:
            try:
                # Extract values like "Predicted IV: 0.3526" from description
                predicted_text = description.split('Predicted IV:')[1].split(',')[0].strip()
                predicted_iv = float(predicted_text)
            except:
                predicted_iv = None
        
        # Calculate mispricing direction and percentage
        observed_iv = option_data.get('implied_volatility', 0)
        mispricing_direction = None
        mispricing_percent = None
        
        if predicted_iv is not None and observed_iv is not None:
            mispricing_percent = abs((observed_iv - predicted_iv) / predicted_iv * 100)
            mispricing_direction = "Overpriced" if observed_iv > predicted_iv else "Underpriced"
        
        # Generate trade recommendation
        trade_recommendation = generate_trade_recommendation(
            option_type=option_data.get('option_type', ''),
            mispricing_direction=mispricing_direction,
            confidence=anomaly.get('confidence', 0)
        )
        
        # Create record
        record = {
            'timestamp': anomaly.get('timestamp', 0),
            'symbol': option_data.get('symbol', ''),
            'underlying': option_data.get('underlying', ''),
            'option_type': option_data.get('option_type', '').upper(),
            'strike': option_data.get('strike', 0.0),
            'expiration': option_data.get('expiration', 0.0),
            'underlying_price': option_data.get('underlying_price', 0.0),
            'bid': option_data.get('bid', 0.0),
            'ask': option_data.get('ask', 0.0),
            'implied_volatility': option_data.get('implied_volatility', 0.0) * 100,  # Convert to percentage
            'predicted_iv': None if predicted_iv is None else predicted_iv * 100,  # Convert to percentage
            'mispricing_direction': mispricing_direction,
            'mispricing_percent': mispricing_percent,
            'anomaly_score': anomaly.get('anomaly_score', 0.0),
            'anomaly_type': anomaly.get('anomaly_type', ''),
            'description': description,
            'confidence': anomaly.get('confidence', 0.0),
            'trade_recommendation': trade_recommendation
        }
        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Add derived columns
    if not df.empty:
        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        
        # Calculate moneyness
        df['moneyness'] = df['strike'] / df['underlying_price']
        
        # Format expiration in days
        df['expiry_days'] = df['expiration'] * 365.0
        
        # Sort by timestamp descending
        df = df.sort_values('timestamp', ascending=False)
    
    return df


def generate_trade_recommendation(option_type: str, mispricing_direction: Optional[str], confidence: float) -> str:
    """Generate a trade recommendation based on option type and mispricing direction"""
    if mispricing_direction is None:
        return "Insufficient data"
    
    if confidence < 0.5:
        return "Low confidence - Avoid"
    
    # Generate recommendation
    if option_type.lower() == 'call':
        if mispricing_direction == "Overpriced":
            if confidence > 0.8:
                return "STRONG SELL - Overpriced call"
            else:
                return "Consider selling call or bear call spread"
        else:  # Underpriced
            if confidence > 0.8:
                return "STRONG BUY - Underpriced call"
            else:
                return "Consider buying call or bull call spread"
    
    elif option_type.lower() == 'put':
        if mispricing_direction == "Overpriced":
            if confidence > 0.8:
                return "STRONG SELL - Overpriced put"
            else:
                return "Consider selling put or bear put spread"
        else:  # Underpriced
            if confidence > 0.8:
                return "STRONG BUY - Underpriced put"
            else:
                return "Consider buying put or bull put spread"
    
    return "Unknown option type"


# Enhanced styling for the table
def get_mispricing_style(direction):
    """Get background color style based on mispricing direction"""
    if direction == "Overpriced":
        return {'backgroundColor': 'rgba(255, 99, 71, 0.2)', 'color': 'white'}
    elif direction == "Underpriced":
        return {'backgroundColor': 'rgba(144, 238, 144, 0.2)', 'color': 'white'}
    return {}


def format_iv_cell(observed_iv, predicted_iv, direction):
    """Format IV cell with comparison to predicted IV"""
    if predicted_iv is None:
        return f"{observed_iv:.1f}%"
    
    # Arrow for direction
    arrow = "↑" if direction == "Overpriced" else "↓"
    color = "tomato" if direction == "Overpriced" else "lightgreen"
    
    return html.Div([
        html.Div(f"{observed_iv:.1f}%", style={"fontWeight": "bold"}),
        html.Div([
            html.Span("vs ", style={"fontSize": "smaller"}),
            html.Span(f"{predicted_iv:.1f}%", style={"fontSize": "smaller"})
        ]),
        html.Div(arrow, style={"color": color, "fontSize": "16px", "fontWeight": "bold"})
    ])


# Define the app layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Options Pricing Anomaly Detection", className="mt-3 mb-3"),
            html.P("Real-time detection of mispriced options using IV surface analysis and machine learning"),
        ], width=12)
    ]),
    
    # Stats Cards Row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Total Anomalies Detected", className="card-title"),
                    html.H2(id="total-anomalies", className="card-value text-center mt-3")
                ])
            ], className="stats-card")
        ], width=4),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Options Processed", className="card-title"),
                    html.H2(id="options-processed", className="card-value text-center mt-3")
                ])
            ], className="stats-card")
        ], width=4),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("System Uptime", className="card-title"),
                    html.H2(id="system-uptime", className="card-value text-center mt-3")
                ])
            ], className="stats-card")
        ], width=4)
    ], className="mb-4"),
    
    # Filters Row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Label("Underlying:"),
                            dcc.Dropdown(
                                id="underlying-filter",
                                options=[],
                                multi=True,
                                placeholder="All Underlyings",
                                className="mb-2"
                            )
                        ], width=3),
                        
                        dbc.Col([
                            html.Label("Option Type:"),
                            dcc.Dropdown(
                                id="option-type-filter",
                                options=[
                                    {"label": "Call", "value": "CALL"},
                                    {"label": "Put", "value": "PUT"}
                                ],
                                multi=True,
                                placeholder="All Types",
                                className="mb-2"
                            )
                        ], width=2),
                        
                        dbc.Col([
                            html.Label("Anomaly Type:"),
                            dcc.Dropdown(
                                id="anomaly-type-filter",
                                options=[],
                                multi=True,
                                placeholder="All Anomalies",
                                className="mb-2"
                            )
                        ], width=2),
                        
                        dbc.Col([
                            html.Label("Mispricing:"),
                            dcc.Dropdown(
                                id="mispricing-filter",
                                options=[
                                    {"label": "Overpriced", "value": "Overpriced"},
                                    {"label": "Underpriced", "value": "Underpriced"}
                                ],
                                multi=True,
                                placeholder="Both",
                                className="mb-2"
                            )
                        ], width=2),
                        
                        dbc.Col([
                            html.Label("Trade Action:"),
                            dcc.Dropdown(
                                id="trade-action-filter",
                                options=[
                                    {"label": "Strong Buy", "value": "STRONG BUY"},
                                    {"label": "Strong Sell", "value": "STRONG SELL"},
                                    {"label": "Consider Buy", "value": "Consider buying"},
                                    {"label": "Consider Sell", "value": "Consider selling"}
                                ],
                                multi=True,
                                placeholder="All Actions",
                                className="mb-2"
                            )
                        ], width=3)
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            html.Label("Min Confidence:"),
                            dcc.Slider(
                                id="confidence-slider",
                                min=0,
                                max=1,
                                step=0.05,
                                value=0.5,
                                marks={0: "0", 0.25: "0.25", 0.5: "0.5", 0.75: "0.75", 1: "1"},
                                className="mb-2"
                            )
                        ], width=6),
                        
                        dbc.Col([
                            html.Label("Time Range:"),
                            dcc.Dropdown(
                                id="time-range-filter",
                                options=[
                                    {"label": "Last 15 minutes", "value": 15},
                                    {"label": "Last hour", "value": 60},
                                    {"label": "Last 4 hours", "value": 240},
                                    {"label": "Last day", "value": 1440},
                                    {"label": "All time", "value": 0}
                                ],
                                value=60,
                                className="mb-2"
                            )
                        ], width=6)
                    ])
                ])
            ], className="filter-card")
        ], width=12)
    ], className="mb-4"),
    
    # Main content - Graphs and Tables
    dbc.Row([
        # Anomalies Table
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H4("Trading Opportunities", className="d-inline-block"),
                    html.Div([
                        html.Span("⬆️ Underpriced", className="legend-item", style={"color": "lightgreen", "marginRight": "15px"}),
                        html.Span("⬇️ Overpriced", className="legend-item", style={"color": "tomato"}),
                    ], className="float-right")
                ]),
                dbc.CardBody([
                    html.Div(id="anomalies-table", className="anomalies-table")
                ])
            ], className="h-100")
        ], width=12, className="mb-4"),
        
        # Charts row 1
        dbc.Row([
            # Anomaly Score Distribution
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H4("Anomaly Score Distribution")),
                    dbc.CardBody([
                        dcc.Graph(id="anomaly-score-chart", config={"displayModeBar": False})
                    ])
                ], className="h-100")
            ], width=6, className="mb-4"),
            
            # IV Surface Anomalies
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H4("IV Surface Anomalies")),
                    dbc.CardBody([
                        dcc.Graph(id="iv-surface-chart", config={"displayModeBar": False})
                    ])
                ], className="h-100")
            ], width=6, className="mb-4"),
        ]),
        
        # Charts row 2
        dbc.Row([
            # Anomalies by Underlying
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H4("Anomalies by Underlying")),
                    dbc.CardBody([
                        dcc.Graph(id="underlying-chart", config={"displayModeBar": False})
                    ])
                ], className="h-100")
            ], width=6, className="mb-4"),
            
            # Anomalies Timeline
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H4("Anomalies Timeline")),
                    dbc.CardBody([
                        dcc.Graph(id="timeline-chart", config={"displayModeBar": False})
                    ])
                ], className="h-100")
            ], width=6, className="mb-4")
        ])
    ]),
    
    # Hidden div for storing the data
    html.Div(id="anomalies-store", style={"display": "none"}),
    html.Div(id="stats-store", style={"display": "none"}),
    
    # Interval component for refreshing the data
    dcc.Interval(
        id="interval-component",
        interval=REFRESH_INTERVAL,
        n_intervals=0
    )
], fluid=True)


# Define callbacks
@app.callback(
    [Output("anomalies-store", "children"),
     Output("stats-store", "children")],
    [Input("interval-component", "n_intervals")]
)
def update_data(n):
    """Fetch data from API and store it"""
    anomalies = fetch_recent_anomalies()
    stats = fetch_stats()
    
    return json.dumps(anomalies), json.dumps(stats)


@app.callback(
    [Output("underlying-filter", "options"),
     Output("anomaly-type-filter", "options")],
    [Input("anomalies-store", "children")]
)
def update_filters(anomalies_json):
    """Update filter options based on available data"""
    if not anomalies_json:
        return [], []
    
    anomalies = json.loads(anomalies_json)
    df = prepare_anomalies_dataframe(anomalies)
    
    if df.empty:
        return [], []
    
    # Get unique underlyings
    underlyings = df['underlying'].unique().tolist()
    underlying_options = [{"label": u, "value": u} for u in sorted(underlyings)]
    
    # Get unique anomaly types
    anomaly_types = df['anomaly_type'].unique().tolist()
    anomaly_type_options = [{"label": t, "value": t} for t in sorted(anomaly_types)]
    
    return underlying_options, anomaly_type_options


@app.callback(
    [Output("total-anomalies", "children"),
     Output("options-processed", "children"),
     Output("system-uptime", "children")],
    [Input("stats-store", "children")]
)
def update_stats_cards(stats_json):
    """Update statistics cards"""
    if not stats_json:
        return "0", "0", "0:00:00"
    
    stats = json.loads(stats_json)
    
    # Format uptime as HH:MM:SS
    uptime_seconds = stats.get("uptime", 0)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
    
    return (
        f"{stats.get('detected_anomalies', 0):,}",
        f"{stats.get('processed_options', 0):,}",
        uptime_str
    )


@app.callback(
    Output("anomalies-table", "children"),
    [Input("anomalies-store", "children"),
     Input("underlying-filter", "value"),
     Input("option-type-filter", "value"),
     Input("anomaly-type-filter", "value"),
     Input("mispricing-filter", "value"),
     Input("trade-action-filter", "value"),
     Input("confidence-slider", "value"),
     Input("time-range-filter", "value")]
)
def update_anomalies_table(anomalies_json, underlying_filter, option_type_filter, 
                           anomaly_type_filter, mispricing_filter, trade_action_filter,
                           confidence_threshold, time_range):
    """Update the anomalies table based on filters"""
    if not anomalies_json:
        return html.P("No anomalies detected yet.")
    
    anomalies = json.loads(anomalies_json)
    df = prepare_anomalies_dataframe(anomalies)
    
    if df.empty:
        return html.P("No anomalies detected yet.")
    
    # Apply filters
    filtered_df = apply_filters(
        df, underlying_filter, option_type_filter, 
        anomaly_type_filter, mispricing_filter, trade_action_filter,
        confidence_threshold, time_range
    )
    
    if filtered_df.empty:
        return html.P("No anomalies match the selected filters.")
    
    # Create table rows
    rows = []
    for _, row in filtered_df.iterrows():
        # Format time
        time_str = row['datetime'].strftime("%H:%M:%S")
        date_str = row['datetime'].strftime("%Y-%m-%d")
        
        # Format the IV cell with comparison
        iv_cell = format_iv_cell(
            row['implied_volatility'],
            row['predicted_iv'],
            row['mispricing_direction']
        )
        
        # Get row style based on mispricing direction
        row_style = get_mispricing_style(row['mispricing_direction'])
        
        # Create row
        table_row = html.Tr([
            html.Td(f"{date_str} {time_str}"),
            html.Td(row['symbol']),
            html.Td(row['underlying']),
            html.Td(f"${row['underlying_price']:.2f}"),
            html.Td(row['option_type']),
            html.Td(f"${row['strike']:.2f}"),
            html.Td(f"${row['bid']:.2f}"),
            html.Td(f"${row['ask']:.2f}"),
            html.Td(f"{row['expiry_days']:.1f}"),
            html.Td(iv_cell),
            html.Td(row['mispricing_direction'] if row['mispricing_direction'] else "Unknown", style=row_style),
            html.Td(f"{row['anomaly_score']:.2f}"),
            html.Td(
                html.Div(
                    className="confidence-bar", 
                    style={"width": f"{row['confidence']*100}%", "background-color": confidence_color(row['confidence'])}
                ),
                className="confidence-cell"
            ),
            html.Td(row['trade_recommendation'], style={"fontWeight": "bold"})
        ], style=row_style)
        rows.append(table_row)
    
    # Create table
    table = html.Table([
        html.Thead(
            html.Tr([
                html.Th("Time"),
                html.Th("Symbol"),
                html.Th("Underlying"),
                html.Th("Undl. Price"),
                html.Th("Type"),
                html.Th("Strike"),
                html.Th("Bid"),
                html.Th("Ask"),
                html.Th("Expiry (days)"),
                html.Th("IV vs Predicted"),
                html.Th("Mispricing"),
                html.Th("Score"),
                html.Th("Confidence"),
                html.Th("Trade Action")
            ])
        ),
        html.Tbody(rows)
    ], className="table table-striped table-hover")
    
    return table


@app.callback(
    Output("anomaly-score-chart", "figure"),
    [Input("anomalies-store", "children"),
     Input("underlying-filter", "value"),
     Input("option-type-filter", "value"),
     Input("anomaly-type-filter", "value"),
     Input("mispricing-filter", "value"),
     Input("confidence-slider", "value"),
     Input("time-range-filter", "value")]
)
def update_anomaly_score_chart(anomalies_json, underlying_filter, option_type_filter, 
                               anomaly_type_filter, mispricing_filter, confidence_threshold, time_range):
    """Update the anomaly score distribution chart"""
    if not anomalies_json:
        return empty_figure("No data available")
    
    anomalies = json.loads(anomalies_json)
    df = prepare_anomalies_dataframe(anomalies)
    
    if df.empty:
        return empty_figure("No anomalies detected yet")
    
    # Apply filters
    filtered_df = apply_filters(
        df, underlying_filter, option_type_filter, 
        anomaly_type_filter, mispricing_filter, None,
        confidence_threshold, time_range
    )
    
    if filtered_df.empty:
        return empty_figure("No anomalies match the selected filters")
    
    # Create histogram colored by mispricing direction
    fig = px.histogram(
        filtered_df,
        x="anomaly_score",
        color="mispricing_direction",
        nbins=20,
        opacity=0.7,
        color_discrete_map={
            "Overpriced": "tomato",
            "Underpriced": "lightgreen",
            None: "gray"
        }
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Anomaly Score",
        yaxis_title="Count",
        legend_title="Mispricing Direction",
        template="plotly_dark",
        margin=dict(l=40, r=20, t=20, b=40),
        legend=dict(orientation="h", y=-0.2)
    )
    
    return fig


@app.callback(
    Output("iv-surface-chart", "figure"),
    [Input("anomalies-store", "children"),
     Input("underlying-filter", "value"),
     Input("option-type-filter", "value"),
     Input("anomaly-type-filter", "value"),
     Input("mispricing-filter", "value"),
     Input("confidence-slider", "value"),
     Input("time-range-filter", "value")]
)
def update_iv_surface_chart(anomalies_json, underlying_filter, option_type_filter, 
                           anomaly_type_filter, mispricing_filter, confidence_threshold, time_range):
    """Update the IV surface anomalies chart"""
    if not anomalies_json:
        return empty_figure("No data available")
    
    anomalies = json.loads(anomalies_json)
    df = prepare_anomalies_dataframe(anomalies)
    
    if df.empty:
        return empty_figure("No anomalies detected yet")
    
    # Apply filters
    filtered_df = apply_filters(
        df, underlying_filter, option_type_filter, 
        anomaly_type_filter, mispricing_filter, None,
        confidence_threshold, time_range
    )
    
    if filtered_df.empty:
        return empty_figure("No anomalies match the selected filters")
    
    # Filter to only IV surface anomalies if any exist
    iv_anomalies = filtered_df[filtered_df['anomaly_type'] == 'iv_surface']
    
    if not iv_anomalies.empty:
        df_to_plot = iv_anomalies
    else:
        # If no IV surface anomalies, use all anomalies
        df_to_plot = filtered_df
    
    # Create scatter plot of moneyness vs. expiration
    fig = px.scatter(
        df_to_plot,
        x="moneyness",
        y="expiry_days",
        color="implied_volatility",
        size="anomaly_score",
        hover_name="symbol",
        hover_data={
            "underlying": True,
            "underlying_price": True,
            "strike": True,
            "option_type": True,
            "implied_volatility": ":.2f",
            "predicted_iv": ":.2f",
            "mispricing_direction": True,
            "bid": ":.2f",
            "ask": ":.2f",
            "anomaly_score": ":.4f",
            "confidence": ":.2f"
        },
        color_continuous_scale="Viridis",
        opacity=0.8,
        symbol="option_type",
        symbol_map={"CALL": "circle", "PUT": "square"}
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Moneyness (Strike/Underlying)",
        yaxis_title="Days to Expiration",
        coloraxis_colorbar_title="Implied Volatility",
        template="plotly_dark",
        margin=dict(l=40, r=20, t=20, b=40)
    )
    
    # Log scale for y-axis (days to expiration)
    fig.update_yaxes(type="log")
    
    return fig


@app.callback(
    Output("underlying-chart", "figure"),
    [Input("anomalies-store", "children"),
     Input("underlying-filter", "value"),
     Input("option-type-filter", "value"),
     Input("anomaly-type-filter", "value"),
     Input("mispricing-filter", "value"),
     Input("confidence-slider", "value"),
     Input("time-range-filter", "value")]
)
def update_underlying_chart(anomalies_json, underlying_filter, option_type_filter, 
                           anomaly_type_filter, mispricing_filter, confidence_threshold, time_range):
    """Update the anomalies by underlying chart"""
    if not anomalies_json:
        return empty_figure("No data available")
    
    anomalies = json.loads(anomalies_json)
    df = prepare_anomalies_dataframe(anomalies)
    
    if df.empty:
        return empty_figure("No anomalies detected yet")
    
    # Apply filters
    filtered_df = apply_filters(
        df, underlying_filter, option_type_filter, 
        anomaly_type_filter, mispricing_filter, None,
        confidence_threshold, time_range
    )
    
    if filtered_df.empty:
        return empty_figure("No anomalies match the selected filters")
    
    # Group by underlying and mispricing direction
    grouped = filtered_df.groupby(['underlying', 'mispricing_direction']).size().reset_index(name='count')
    
    # Create bar chart
    fig = px.bar(
        grouped,
        x="underlying",
        y="count",
        color="mispricing_direction",
        barmode="group",
        text="count",
        color_discrete_map={
            "Overpriced": "tomato",
            "Underpriced": "lightgreen",
            None: "gray"
        }
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Underlying",
        yaxis_title="Number of Anomalies",
        legend_title="Mispricing Direction",
        template="plotly_dark",
        margin=dict(l=40, r=20, t=20, b=40)
    )
    
    # Show text on bars
    fig.update_traces(textposition='outside')
    
    return fig


@app.callback(
    Output("timeline-chart", "figure"),
    [Input("anomalies-store", "children"),
     Input("underlying-filter", "value"),
     Input("option-type-filter", "value"),
     Input("anomaly-type-filter", "value"),
     Input("mispricing-filter", "value"),
     Input("confidence-slider", "value"),
     Input("time-range-filter", "value")]
)
def update_timeline_chart(anomalies_json, underlying_filter, option_type_filter, 
                         anomaly_type_filter, mispricing_filter, confidence_threshold, time_range):
    """Update the anomalies timeline chart"""
    if not anomalies_json:
        return empty_figure("No data available")
    
    anomalies = json.loads(anomalies_json)
    df = prepare_anomalies_dataframe(anomalies)
    
    if df.empty:
        return empty_figure("No anomalies detected yet")
    
    # Apply filters
    filtered_df = apply_filters(
        df, underlying_filter, option_type_filter, 
        anomaly_type_filter, mispricing_filter, None,
        confidence_threshold, time_range
    )
    
    if filtered_df.empty:
        return empty_figure("No anomalies match the selected filters")
    
    # Group by timestamp (binned) and mispricing direction
    filtered_df['time_bin'] = pd.to_datetime(filtered_df['timestamp'], unit='s').dt.floor('5min')
    grouped = filtered_df.groupby(['time_bin', 'mispricing_direction']).size().reset_index(name='count')
    
    # Create line chart
    fig = px.line(
        grouped,
        x="time_bin",
        y="count",
        color="mispricing_direction",
        markers=True,
        color_discrete_map={
            "Overpriced": "tomato",
            "Underpriced": "lightgreen",
            None: "gray"
        }
    )
    
    # Update layout
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Anomalies detected",
        legend_title="Mispricing Direction",
        template="plotly_dark",
        margin=dict(l=40, r=20, t=20, b=40)
    )
    
    return fig


# Helper functions
def apply_filters(df, underlying_filter, option_type_filter, 
                 anomaly_type_filter, mispricing_filter, trade_action_filter,
                 confidence_threshold, time_range):
    """Apply filters to the dataframe"""
    filtered_df = df.copy()
    
    # Confidence filter
    filtered_df = filtered_df[filtered_df['confidence'] >= confidence_threshold]
    
    # Underlying filter
    if underlying_filter:
        filtered_df = filtered_df[filtered_df['underlying'].isin(underlying_filter)]
    
    # Option type filter
    if option_type_filter:
        filtered_df = filtered_df[filtered_df['option_type'].isin(option_type_filter)]
    
    # Anomaly type filter
    if anomaly_type_filter:
        filtered_df = filtered_df[filtered_df['anomaly_type'].isin(anomaly_type_filter)]
    
    # Mispricing direction filter
    if mispricing_filter:
        filtered_df = filtered_df[filtered_df['mispricing_direction'].isin(mispricing_filter)]
    
    # Trade action filter
    if trade_action_filter:
        # Need partial matching for trade recommendations
        mask = filtered_df['trade_recommendation'].apply(
            lambda x: any(action in x for action in trade_action_filter)
        )
        filtered_df = filtered_df[mask]
    
    # Time range filter
    if time_range > 0:
        cutoff_time = pd.Timestamp.now() - pd.Timedelta(minutes=time_range)
        filtered_df = filtered_df[filtered_df['datetime'] > cutoff_time]
    
    return filtered_df


def empty_figure(message):
    """Create an empty figure with a message"""
    fig = go.Figure()
    
    fig.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 20}
            }
        ],
        template="plotly_dark"
    )
    
    return fig


def confidence_color(confidence):
    """Get color based on confidence level"""
    if confidence < 0.5:
        return "#FFA07A"  # Light salmon
    elif confidence < 0.75:
        return "#FFA500"  # Orange
    else:
        return "#32CD32"  # Lime green


# Add CSS for styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .stats-card {
                background-color: #2C3E50;
                border-radius: 10px;
                margin-bottom: 15px;
            }
            .filter-card {
                background-color: #34495E;
                border-radius: 10px;
            }
            .card-value {
                font-size: 2.2rem;
                font-weight: bold;
                color: #3498DB;
            }
            .anomalies-table {
                max-height: 400px;
                overflow-y: auto;
            }
            .confidence-cell {
                width: 100px;
                background-color: #212529;
                padding: 0 !important;
            }
            .confidence-bar {
                height: 20px;
                max-width: 100%;
            }
            .table {
                color: #ECF0F1;
            }
            .table thead th {
                position: sticky;
                top: 0;
                background-color: #2C3E50;
                z-index: 10;
            }
            .legend-item {
                display: inline-block;
                padding: 2px 6px;
                margin-right: 8px;
                font-size: 0.85rem;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''


# Start the app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)