import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import threading
import time
from src.ConnectionLogger import ConnectionLogger  # Import the ConnectionLogger class

SPEEDTEST_SLEEP_TIME_SEC = 60
LOG_CSV_SAVE_PATH = "logs/connection_log.csv"

@st.cache_resource
def get_logger():
    return ConnectionLogger(
        sleep_sec=SPEEDTEST_SLEEP_TIME_SEC,
        outpath=LOG_CSV_SAVE_PATH
    )

# Global variable for the connection logger
logger = get_logger()

def start_logger():
    global logger
    if not logger.is_alive():
        logger.start()

def get_data() -> pd.DataFrame:
    """Retrieve results as a pandas DataFrame."""
    if logger is not None:
        return logger.get_results()
    else:
        return pd.DataFrame(columns=[
            'system_time', 'timestamp', 'download', 'upload', 'ping', 'server_url', 'server_lat', 
            'server_lon', 'server_name', 'server_cc', 'server_id', 'server_d', 
            'server_latency', 'bytes_sent', 'bytes_received', 'share', 
            'client_ip', 'client_lat', 'client_lon', 'client_isp', 'client_isprating', 
            'client_rating', 'client_ispdlavg', 'client_ispulavg', 'client_loggedin', 
            'client_country'
        ])

def health_check():
    """Periodically check the health of the logger and restart if necessary."""
    global logger
    while True:
        if logger is None or not logger.is_alive():
            logger = get_logger()
            logger.start()
        time.sleep(30)  # Check every 30 seconds

def create_speed_chart(df: pd.DataFrame) -> go.Figure:
    """Create a Plotly chart with the given DataFrame."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['system_time'], y=df['download'], mode='lines', name='Download Speed (Mb/s)'))
    fig.add_trace(go.Scatter(x=df['system_time'], y=df['upload'], mode='lines', name='Upload Speed (Mb/s)'))
    fig.update_layout(
        title='Internet Speed Over Time',
        xaxis_title='Time',
        yaxis_title='Speed (Mb/s)',
        yaxis=dict(
            range=[0, None]  # Set y-axis to start from 0 and auto-scale the upper bound
        ),
        legend=dict(
            orientation='h',  # Horizontal orientation
            yanchor='top',    # Anchor the legend to the top of the chart
            y=-0.2,           # Move the legend below the x-axis
            xanchor='center', # Center the legend horizontally
            x=0.5             # Position the legend at the center of the x-axis
        )
    )
    return fig

def csv_download_button(filepath: str):
    """Load a CSV file from disk."""
    if pd.io.common.file_exists(filepath):
        csv_data = pd.read_csv(filepath)
        st.download_button(
            label=f"Download connection speed log (csv, {len(csv_data)} rows)",
            data=csv_data.to_csv(index=False),
            file_name="connection_log.csv",
            mime="text/csv"
        )
    else:
        st.write("Log file not found.")

def main():
    start_logger()

    st.title("Internet Speed Logger")

    # Refresh button
    if st.button("Refresh Chart"):
        df = get_data()
        if not df.empty:
            fig = create_speed_chart(df)
            st.plotly_chart(fig)
        else:
            st.write("No data available.")

    # Download CSV button
    # Check if file exists and enable download button if it does
    csv_download_button(LOG_CSV_SAVE_PATH)


# Start the logger and health check thread on app start
if __name__ == "__main__":
    main()
