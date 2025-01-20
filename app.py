import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
import io
from collections import Counter
import os
from dotenv import load_dotenv
from config import CLICKHOUSE_CONFIG

# Load environment variables
load_dotenv()

# Set page config
st.set_page_config(
    page_title="Device Usage Analytics",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .stMetric {
        background-color: #000000;
        padding: 10px;
        border-radius: 10px;
    }
    .stMetric:hover {
        background-color: #000000;
    }
    .plot-container {
        border-radius: 10px;
        background-color: black;
        padding: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .st-emotion-cache-16idsys p {
        font-size: 14px;
        margin-bottom: 10px;
    }
    div[data-testid="stMetricValue"] > div {
        font-size: 1.8rem;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'df' not in st.session_state:
    st.session_state.df = None

def fetch_data(start_date, end_date):
    url = os.getenv('CLICKHOUSE_URL') or CLICKHOUSE_CONFIG['url']
    query = f"""
    SELECT 
        partner,
        token,
        package,
        last_time_used,
        last_time_foreground_service_used,
        first_time_stamp,
        last_time_stamp,
        last_time_visible,
        total_time_foreground_service_used,
        total_time_in_foreground,
        total_time_visible,
        device['hardware_id'] as hardware_id,
        device['model'] as model,
        device['os'] as os,
        device['product'] as product,
        device['brand'] as brand
    FROM device_usage_stat
    WHERE last_time_used BETWEEN '{start_date}' AND '{end_date}'
    FORMAT CSVWithNames
    """
    
    try:
        response = requests.post(
            url,
            params={'database': os.getenv('CLICKHOUSE_DATABASE') or CLICKHOUSE_CONFIG['database']},
            data=query,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        if st.session_state.get('debug_mode', False):
            st.write("Response Status Code:", response.status_code)
            st.write("Response Headers:", dict(response.headers))
            st.write("Response Preview:", response.text[:500])
        
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text))
        
        # Convert timestamp columns
        timestamp_cols = [
            'last_time_used', 'last_time_foreground_service_used',
            'first_time_stamp', 'last_time_stamp', 'last_time_visible'
        ]
        for col in timestamp_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert numeric columns
        numeric_cols = [
            'total_time_foreground_service_used',
            'total_time_in_foreground',
            'total_time_visible'
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert string columns to string type
        string_cols = ['partner', 'token', 'package', 'hardware_id', 'model', 'os', 'product', 'brand']
        for col in string_cols:
            df[col] = df[col].astype(str)
        
        return df
    
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return None

def calculate_app_metrics(df):
    """Calculate detailed app usage metrics"""
    metrics = df.groupby('package').agg({
        'hardware_id': 'nunique',  # Reach (unique devices)
        'total_time_in_foreground': ['sum', 'mean'],  # Total and average time
    }).reset_index()
    
    metrics.columns = ['package', 'reach', 'total_time', 'avg_time_per_session']
    metrics['avg_time_per_device'] = metrics['total_time'] / metrics['reach']
    
    # Convert times from seconds to hours
    for col in ['total_time', 'avg_time_per_session', 'avg_time_per_device']:
        metrics[col] = metrics[col] / 3600
    
    metrics = metrics.round(2)
    return metrics.sort_values('reach', ascending=False)

# Main title and description
st.title("📱 Device Usage Analytics Dashboard")
st.markdown("""
    <div style='background-color: #000000; padding: 15px; border-radius: 10px; margin-bottom: 25px;'>
        <h4 style='margin-top: 0;'>Welcome to the Device Usage Analytics Dashboard</h4>
        <p>This dashboard provides comprehensive insights into device usage patterns and app engagement metrics. 
        Select a date range below to begin your analysis.</p>
    </div>
""", unsafe_allow_html=True)

# Date Selection
col1, col2, col3 = st.columns([2, 2, 1])

with col1:
    start_date = st.date_input(
        "Start Date",
        value=datetime.now() - timedelta(days=7),
        max_value=datetime.now()
    )

with col2:
    end_date = st.date_input(
        "End Date",
        value=datetime.now(),
        max_value=datetime.now()
    )

with col3:
    st.markdown("<br>", unsafe_allow_html=True)  # Add spacing
    if st.button("📥 Load Data", use_container_width=True):
        with st.spinner('Fetching data...'):
            df = fetch_data(start_date, end_date)
            if df is not None and not df.empty:
                st.session_state.df = df
                st.session_state.data_loaded = True
                st.success(f"Successfully loaded {len(df):,} records")
            else:
                st.error("Failed to load data")
                st.session_state.data_loaded = False

# Continue only if data is loaded
if st.session_state.data_loaded and st.session_state.df is not None:
    df = st.session_state.df
    
    # Sidebar filters and controls
    with st.sidebar:
        st.markdown("""
            <div style='text-align: center; margin-bottom: 20px;'>
                <h3>Dashboard Controls</h3>
            </div>
        """, unsafe_allow_html=True)
        
        # Debug mode toggle
        st.session_state.debug_mode = st.checkbox("🔧 Debug Mode", value=False)
        
        st.markdown("---")
        st.header("🎯 Filters")
        
        # Partner filter
        partners = sorted(df['partner'].unique().tolist())
        selected_partners = st.multiselect(
            "Select Partners",
            options=partners,
            default=partners,
            key='partner_filter'
        )
        
        # Brand filter
        brands = sorted(df['brand'].unique().tolist())
        selected_brands = st.multiselect(
            "Select Brands",
            options=brands,
            default=brands,
            key='brand_filter'
        )

    # Apply filters
    filtered_df = df[
        (df['partner'].isin(selected_partners)) &
        (df['brand'].isin(selected_brands))
    ]

    # Calculate app metrics
    app_metrics = calculate_app_metrics(filtered_df)

    # Key Metrics Section
    st.header("📈 Key Performance Indicators")
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Unique Devices",
            f"{len(filtered_df['hardware_id'].unique()):,}",
            help="Number of unique devices in the selected date range"
        )

    with col2:
        st.metric(
            "Total Apps Tracked",
            f"{len(filtered_df['package'].unique()):,}",
            help="Number of unique applications being tracked"
        )

    with col3:
        avg_time = filtered_df['total_time_in_foreground'].mean() / 3600
        st.metric(
            "Avg Usage Time",
            f"{avg_time:.2f} hours",
            help="Average time spent per app"
        )

    with col4:
        active_devices = filtered_df[
            filtered_df['last_time_used'] >= (datetime.now() - timedelta(days=7))
        ]['hardware_id'].nunique()
        st.metric(
            "Active Devices (7d)",
            f"{active_devices:,}",
            help="Devices active in the last 7 days"
        )

    # App Usage Deep Dive
    st.markdown("---")
    st.header("📱 App Usage Analysis")
    
    # Top Apps by Reach and Usage
    col1, col2 = st.columns([2, 1])

    with col1:
        fig = px.bar(
            app_metrics.head(10),
            x='package',
            y='reach',
            title="Top 10 Apps by Reach",
            color='total_time',
            labels={
                'package': 'App Package',
                'reach': 'Number of Unique Devices',
                'total_time': 'Total Usage Time (hours)'
            }
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            hovermode='x unified',
            hoverlabel=dict(bgcolor="black"),
            plot_bgcolor='rgba(0,0,0,0)',
            height=400,
            margin=dict(t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.scatter(
            app_metrics.head(20),
            x='reach',
            y='total_time',
            size='avg_time_per_device',
            color='avg_time_per_session',
            hover_name='package',
            title="Reach vs Usage Matrix",
            labels={
                'reach': 'Reach (Devices)',
                'total_time': 'Total Time (hours)',
                'avg_time_per_session': 'Avg Session (hours)'
            }
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            hoverlabel=dict(bgcolor="black"),
            height=400,
            margin=dict(t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    # Detailed App Metrics Table
    st.markdown("---")
    st.header("📊 Detailed App Metrics")

    # Metric selector and search
    col1, col2 = st.columns([2, 2])
    
    metric_options = {
        "reach": "Reach (Unique Devices)",
        "total_time": "Total Usage Time",
        "avg_time_per_device": "Average Time per Device",
        "avg_time_per_session": "Average Time per Session"
    }

    with col1:
        metric_sort = st.selectbox(
            "Sort Metrics By",
            list(metric_options.keys()),
            format_func=lambda x: metric_options[x]
        )

    with col2:
        search_term = st.text_input(
            "Search Apps",
            placeholder="Enter app package name..."
        )

    # Filter and sort metrics
    if search_term:
        displayed_metrics = app_metrics[
            app_metrics['package'].str.contains(search_term, case=False)
        ]
    else:
        displayed_metrics = app_metrics

    displayed_metrics = displayed_metrics.sort_values(metric_sort, ascending=False).head(20)

    # Display interactive table
    st.dataframe(
        displayed_metrics.style
            .format({
                'reach': '{:,.0f}',
                'total_time': '{:,.2f}',
                'avg_time_per_session': '{:,.2f}',
                'avg_time_per_device': '{:,.2f}'
            })
            .background_gradient(cmap='viridis', subset=['reach', 'total_time'])
            .bar(subset=['avg_time_per_device', 'avg_time_per_session'], color='lightblue'),
        height=400
    )

# Usage Patterns Analysis
    st.markdown("---")
    st.header("📊 Usage Patterns")
    
    col1, col2 = st.columns(2)

    with col1:
        # Hourly usage patterns
        hourly_usage = filtered_df.groupby(
            filtered_df['last_time_used'].dt.hour
        )['total_time_in_foreground'].mean()
        
        fig = px.line(
            x=hourly_usage.index,
            y=hourly_usage.values/3600,
            title="Average Usage by Hour of Day",
            labels={
                'x': 'Hour of Day',
                'y': 'Average Usage Time (hours)'
            },
            markers=True
        )
        fig.update_layout(
            xaxis=dict(tickmode='linear', dtick=1),
            plot_bgcolor='rgba(0,0,0,0)',
            height=400,
            margin=dict(t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Weekly usage patterns
        weekly_usage = filtered_df.groupby(
            filtered_df['last_time_used'].dt.day_name()
        )['total_time_in_foreground'].mean()
        
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekly_usage = weekly_usage.reindex(days_order)
        
        fig = px.bar(
            x=weekly_usage.index,
            y=weekly_usage.values/3600,
            title="Average Usage by Day of Week",
            labels={
                'x': 'Day of Week',
                'y': 'Average Usage Time (hours)'
            },
            color=weekly_usage.values,
            color_continuous_scale='viridis'
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            height=400,
            margin=dict(t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    # Device Statistics
    st.markdown("---")
    st.header("📱 Device Statistics")
    
    col1, col2, col3 = st.columns(3)

    with col1:
        # Top Brands
        brand_stats = filtered_df['brand'].value_counts().head(10)
        fig = px.pie(
            values=brand_stats.values,
            names=brand_stats.index,
            title="Device Brand Distribution",
            hole=0.4
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # OS Distribution
        os_stats = filtered_df['os'].value_counts()
        fig = px.bar(
            x=os_stats.index,
            y=os_stats.values,
            title="Operating System Distribution",
            labels={'x': 'OS', 'y': 'Count'}
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        # Model Distribution
        model_stats = filtered_df['model'].value_counts().head(10)
        fig = px.bar(
            x=model_stats.values,
            y=model_stats.index,
            title="Top 10 Device Models",
            labels={'x': 'Count', 'y': 'Model'},
            orientation='h'
        )
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

    # Export functionality
    st.sidebar.markdown("---")
    st.sidebar.header("📥 Export Options")

    if st.sidebar.button("Prepare Download"):
        csv = app_metrics.to_csv(index=False)
        st.sidebar.download_button(
            label="📥 Download Metrics (CSV)",
            data=csv,
            file_name=f"app_metrics_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

    # Quick Insights
    st.sidebar.markdown("---")
    st.sidebar.header("💡 Quick Insights")

    top_app = app_metrics.iloc[0]
    most_engaged_app = app_metrics.sort_values('avg_time_per_device', ascending=False).iloc[0]

    st.sidebar.markdown(f"""
    **Most Popular App:**  
    {top_app['package']}  
    Reach: {top_app['reach']:,.0f} devices
    
    **Most Engaging App:**  
    {most_engaged_app['package']}  
    Avg Time: {most_engaged_app['avg_time_per_device']:.2f} hours/device
    
    **Total Apps Analyzed:** {len(app_metrics):,}
    """)

    # Debug Information
    if st.session_state.debug_mode:
        st.sidebar.markdown("---")
        st.sidebar.header("🔧 Debug Information")
        with st.sidebar.expander("View Debug Info"):
            st.write("Data Sample:", filtered_df.head())
            st.write("Data Types:", filtered_df.dtypes)
            st.write("Missing Values:", filtered_df.isnull().sum())

    # Footer
    st.markdown("---")
    st.markdown(
        f"""
        <div style='text-align: center; color: #666; padding: 20px;'>
            <p>Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p>Data range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}</p>
        </div>
        """,
        unsafe_allow_html=True
    )

else:
    # Show welcome message when no data is loaded
    st.info("👆 Please select a date range and click 'Load Data' to view the dashboard.")
    
    # Show sample metrics layout
    st.markdown("""
        <div style='background-color: #000000; padding: 20px; border-radius: 10px; margin-top: 20px;'>
            <h4>Dashboard Features:</h4>
            <ul>
                <li>Comprehensive app usage analytics</li>
                <li>Device distribution statistics</li>
                <li>Usage patterns and trends</li>
                <li>Detailed metrics and insights</li>
                <li>Interactive visualizations</li>
                <li>Export capabilities</li>
            </ul>
        </div>
    """, unsafe_allow_html=True)