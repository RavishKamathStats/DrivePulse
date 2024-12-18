import mysql.connector
import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import plotly.express as px
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

# Database connection details
MYSQL_HOST = 'ec2-54-241-86-221.us-west-1.compute.amazonaws.com'
MYSQL_USER = 'driving_user'
MYSQL_PASSWORD = 'dsci560'
MYSQL_DATABASE = 'driving_data'

# Function to fetch data and return a DataFrame
def fetch_data(query, params=None):
    """Fetch data from MySQL database and return it as a DataFrame."""
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            auth_plugin='mysql_native_password',
            connection_timeout=10
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params or ())
        results = cursor.fetchall()
        return pd.DataFrame(results)
    except mysql.connector.Error as err:
        st.error(f"Database query error: {err}")
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()
        if connection.is_connected():
            connection.close()

# Fetch distinct trip IDs for the dropdown menu
def get_trip_ids():
    query = "SELECT DISTINCT trip_id FROM device_trip_mapping"
    trip_data = fetch_data(query)
    return trip_data['trip_id'].tolist() if not trip_data.empty else []

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import streamlit as st

def plot_score_bar(final_score):
    fig, ax = plt.subplots(figsize=(8, 0.5), facecolor='none')
    cmap = LinearSegmentedColormap.from_list("score_gradient", ["red", "yellow", "green"])
    norm = plt.Normalize(10, 100)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])

    # Draw gradient bar
    gradient = np.linspace(10, 100, 256)
    gradient = np.vstack((gradient, gradient))
    ax.imshow(gradient, aspect='auto', cmap=cmap, extent=[10, 100, 0, 1])

    # Mark the final score with vertical line
    ax.axvline(final_score, color='black', linestyle='--', linewidth=2)

    # Add a small pointer (arrow) at the top
    ax.annotate(
        '',  # No text, only an arrow
        xy=(final_score, 1.05),   # Arrow tip (x-coordinate of score)
        xytext=(final_score, 1.2),  # Arrow base
        arrowprops=dict(arrowstyle="->", color='white', linewidth=1.5)
    )

    # Add text for the score above the pointer
    ax.text(final_score, 1.3, f"{final_score:.0f}", color='white', fontsize=12, ha='center')

    # Customize axis
    ax.set_xticks([10, 25, 50, 75, 100])
    ax.tick_params(axis='x', colors='white')
    ax.set_yticks([])
    ax.set_xlim(10, 100)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)

    # Remove the background
    fig.patch.set_facecolor('none')
    ax.patch.set_facecolor('none')

    # Show the plot in Streamlit
    st.pyplot(fig, transparent=True)


# Streamlit app UI
st.title("Driving Behavior Dashboard")

# Sidebar Filters



   
st.sidebar.header("Filters")
trip_ids = get_trip_ids()
selected_trip = st.sidebar.selectbox("Select Trip ID", trip_ids)

# Display Driving Scores
if selected_trip:
    st.subheader(f"Driving Scores for Trip ID: {selected_trip}")
    scores_query = """
    SELECT acceleration_score, braking_score, speeding_score, cornering_score, final_score
    FROM scores_data
    WHERE trip_id = %s
    """
    scores_data = fetch_data(scores_query, (selected_trip,))
    if not scores_data.empty:
        # Display the final score prominently
        final_score = scores_data['final_score'].iloc[0]
        st.metric("Final Driving Score", f"{final_score:.2f}")
        plot_score_bar(final_score)

        # Display other scores in a table
        st.subheader("Detailed Scores")
        st.dataframe(scores_data[['acceleration_score', 'braking_score', 'speeding_score', 'cornering_score']])
    else:
        st.warning("No scores data found for this Trip ID.")


# Penalty Events and Route Visualization (Folium Map)
st.subheader("Route and Penalty Events Map")
events_query = """
    SELECT latitude, longitude, acceleration_event, braking_event, speeding_event, cornering_event
    FROM penalty_events_data
    WHERE trip_id = %s AND (
        acceleration_event = 'severe' OR
        braking_event = 'severe' OR
        speeding_event = 'severe' OR
        cornering_event = 'severe'
    )
"""
route_query = """
    SELECT latitude, longitude
    FROM driving_data
    WHERE trip_id = %s
    ORDER BY timestamp
"""
events_data = fetch_data(events_query, (selected_trip,))
route_data = fetch_data(route_query, (selected_trip,))

if not route_data.empty:
    # Create a Folium map centered on the route
    m = folium.Map(location=[route_data['latitude'].mean(), route_data['longitude'].mean()], zoom_start=12)

    # Plot the route as a PolyLine
    route_coordinates = list(zip(route_data['latitude'], route_data['longitude']))
    folium.PolyLine(route_coordinates, color="blue", weight=2.5, opacity=1).add_to(m)

    # Add markers for severe penalty events
    if not events_data.empty:
        for _, row in events_data.iterrows():
            #st.write(f"Adding marker for event: {row}")  # Debugging log
            event_info = f"Acceleration: {row['acceleration_event']}, Braking: {row['braking_event']}, " \
                         f"Speeding: {row['speeding_event']}, Cornering: {row['cornering_event']}"
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=event_info,
                icon=folium.Icon(color='red')
            ).add_to(m)

    folium_static(m)
else:
    st.warning("No route data found for this Trip ID.")

# Trip Analytics


# Trip Analytics
st.subheader("Trip Analytics")
trip_query = """
    SELECT timestamp, speed, acceleration, lateral_acceleration, speed_gps_avg, lateral_acceleration_avg
    FROM preprocessed_driving_data
    WHERE trip_id = %s
"""
trip_data = fetch_data(trip_query, (selected_trip,))
if not trip_data.empty:
    trip_data['timestamp'] = pd.to_datetime(trip_data['timestamp'])
    
    # Show summary statistics
    avg_speed = trip_data['speed'].mean()
    max_acceleration = trip_data['acceleration'].max()
    avg_lateral_acceleration = trip_data['lateral_acceleration_avg'].mean()

    st.metric("Average Speed", f"{avg_speed:.2f}")
    st.metric("Max Acceleration", f"{max_acceleration:.2f}")
   

    # Line chart for metrics over time
    fig = px.line(
        trip_data,
        x='timestamp',
        y='speed_gps_avg',
        labels={'timestamp': 'Time', 'speed_gps_avg': 'Speed (miles/hour)'},
        title=f"Trip Metrics for Trip ID: {selected_trip}",
        template='plotly_white'
    )
    st.plotly_chart(fig)
else:
    st.warning("No trip analytics data found for this Trip ID.")
