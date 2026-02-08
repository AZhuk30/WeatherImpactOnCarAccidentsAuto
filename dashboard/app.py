"""
Streamlit Dashboard for NYC Traffic Safety Analysis
STREAMLIT CLOUD READY - Reads from master CSV files
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Page configuration
st.set_page_config(
    page_title="NYC Traffic Safety - Weather Impact",
    page_icon="🚗",
    layout="wide"
)

# Title
st.title("🚗 NYC Traffic Safety - Weather Impact Analysis")
st.markdown("Real-time analysis of weather's impact on NYC traffic collisions")

# Add last update indicator
st.sidebar.markdown("### 📊 Dashboard Info")
st.sidebar.info("Data updates automatically via GitHub Actions daily at 2 AM UTC")

# Load data from master CSV files
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_latest_data():
    """
    Load master CSV files with all accumulated historical data
    Works both locally and on Streamlit Cloud
    """
    
    # Master file paths (always the same)
    weather_master = "data/processed/weather_master.csv"
    collisions_master = "data/processed/collisions_master.csv"
    
    # Check if files exist
    if not os.path.exists(weather_master) or not os.path.exists(collisions_master):
        st.error("""
        ## ⚠️ Data Files Not Found
        
        Master data files are missing. This could mean:
        1. The pipeline hasn't been run yet
        2. Files are not in the expected location
        
        **To fix:**
        - Run: `python run_pipeline.py`
        - Or check that files exist in `data/processed/`
        """)
        return None, None
    
    try:
        # Load master files
        weather_df = pd.read_csv(weather_master)
        collisions_df = pd.read_csv(collisions_master)
        
        # Convert datetime columns and remove timezone for merging
        if 'datetime' in weather_df.columns:
            weather_df['datetime'] = pd.to_datetime(weather_df['datetime'])
            # Remove timezone to allow merging
            if weather_df['datetime'].dt.tz is not None:
                weather_df['datetime'] = weather_df['datetime'].dt.tz_localize(None)
        
        if 'crash_datetime' in collisions_df.columns:
            collisions_df['crash_datetime'] = pd.to_datetime(collisions_df['crash_datetime'])
            # Remove timezone to allow merging
            if collisions_df['crash_datetime'].dt.tz is not None:
                collisions_df['crash_datetime'] = collisions_df['crash_datetime'].dt.tz_localize(None)
        
        # Show data info in sidebar
        if len(weather_df) > 0:
            min_date = weather_df['datetime'].min()
            max_date = weather_df['datetime'].max()
            total_days = (max_date - min_date).days + 1
            
            st.sidebar.success(f"""
            **Data Range:**  
            {min_date.strftime('%b %d, %Y')} to {max_date.strftime('%b %d, %Y')}  
            ({total_days} days)
            
            **Records:**  
            🌤️ Weather: {len(weather_df):,}  
            🚗 Collisions: {len(collisions_df):,}
            """)
        
        return weather_df, collisions_df
        
    except Exception as e:
        st.error(f"""
        ## ❌ Error Loading Data
        
        **Error:** {str(e)}
        
        Please check:
        1. Files exist in `data/processed/`
        2. Files are valid CSV format
        3. Run the pipeline: `python run_pipeline.py`
        """)
        return None, None

# Load data
with st.spinner('Loading data...'):
    weather_df, collisions_df = load_latest_data()

if weather_df is not None and collisions_df is not None:
    
    # ========== SIDEBAR FILTERS ==========
    st.sidebar.header("🔍 Filters")
    
    # Borough filter
    boroughs = ['ALL'] + sorted(collisions_df['borough'].dropna().unique().tolist())
    selected_borough = st.sidebar.selectbox("Select Borough", boroughs)
    
    # Date range filter
    if 'crash_datetime' in collisions_df.columns:
        min_date = collisions_df['crash_datetime'].min().date()
        max_date = collisions_df['crash_datetime'].max().date()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Handle single date selection
        if len(date_range) == 2:
            start_filter, end_filter = date_range
        else:
            start_filter = end_filter = date_range[0]
    
    # Weather condition filter
    if 'weather_category' in weather_df.columns:
        weather_conditions = ['ALL'] + sorted(weather_df['weather_category'].unique().tolist())
        selected_weather = st.sidebar.selectbox("Weather Condition", weather_conditions)
    
    # Apply filters
    filtered_collisions = collisions_df.copy()
    filtered_weather = weather_df.copy()
    
    # Filter by borough
    if selected_borough != 'ALL':
        filtered_collisions = filtered_collisions[filtered_collisions['borough'] == selected_borough]
        filtered_weather = filtered_weather[filtered_weather['borough'] == selected_borough]
    
    # Filter by date range
    if 'crash_datetime' in filtered_collisions.columns:
        filtered_collisions = filtered_collisions[
            (filtered_collisions['crash_datetime'].dt.date >= start_filter) &
            (filtered_collisions['crash_datetime'].dt.date <= end_filter)
        ]
    
    if 'datetime' in filtered_weather.columns:
        filtered_weather = filtered_weather[
            (filtered_weather['datetime'].dt.date >= start_filter) &
            (filtered_weather['datetime'].dt.date <= end_filter)
        ]
    
    # Filter by weather
    if selected_weather != 'ALL':
        filtered_weather = filtered_weather[filtered_weather['weather_category'] == selected_weather]
    
    # ========== DASHBOARD METRICS ==========
    st.header("📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_collisions = len(filtered_collisions)
        st.metric("Total Collisions", f"{total_collisions:,}")
    
    with col2:
        total_injuries = filtered_collisions['persons_injured'].sum() if 'persons_injured' in filtered_collisions.columns else 0
        st.metric("Total Injuries", f"{int(total_injuries):,}")
    
    with col3:
        total_fatalities = filtered_collisions['persons_killed'].sum() if 'persons_killed' in filtered_collisions.columns else 0
        st.metric("Total Fatalities", int(total_fatalities))
    
    with col4:
        if 'severity_level' in filtered_collisions.columns:
            severe_collisions = len(filtered_collisions[filtered_collisions['severity_level'].isin(['SEVERE', 'FATAL'])])
            st.metric("Severe Collisions", severe_collisions)
        else:
            st.metric("Severe Collisions", "N/A")
    
    # ========== VISUALIZATIONS ==========
    st.header("📈 Analysis Visualizations")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Collision Patterns", "Weather Impact", "Time Analysis", "Data Explorer"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            if 'borough' in filtered_collisions.columns and len(filtered_collisions) > 0:
                borough_counts = filtered_collisions['borough'].value_counts().reset_index()
                borough_counts.columns = ['borough', 'count']
                
                fig = px.bar(
                    borough_counts, 
                    x='borough', 
                    y='count',
                    title="Collisions by Borough",
                    color='count',
                    color_continuous_scale='reds',
                    labels={'count': 'Number of Collisions'}
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No collision data for selected filters")
        
        with col2:
            if 'severity_level' in filtered_collisions.columns and len(filtered_collisions) > 0:
                severity_counts = filtered_collisions['severity_level'].value_counts().reset_index()
                severity_counts.columns = ['severity', 'count']
                
                fig = px.pie(
                    severity_counts, 
                    values='count', 
                    names='severity',
                    title="Collision Severity Distribution",
                    hole=0.3,
                    color_discrete_sequence=px.colors.sequential.Reds_r
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No severity data available")
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            if 'weather_category' in filtered_weather.columns and len(filtered_weather) > 0:
                # Round datetime to nearest hour for matching
                weather_hourly = filtered_weather.copy()
                weather_hourly['datetime_hour'] = weather_hourly['datetime'].dt.floor('H')
                
                collisions_hourly = filtered_collisions.copy()
                collisions_hourly['datetime_hour'] = collisions_hourly['crash_datetime'].dt.floor('H')
                
                # Merge on hour + borough
                merged = pd.merge(
                    collisions_hourly,
                    weather_hourly[['borough', 'datetime_hour', 'weather_category']],
                    left_on=['borough', 'datetime_hour'],
                    right_on=['borough', 'datetime_hour'],
                    how='left'
                )
                
                if len(merged) > 0 and 'weather_category' in merged.columns:
                    weather_collisions = merged.groupby('weather_category').size().reset_index()
                    weather_collisions.columns = ['weather', 'collisions']
                    
                    fig = px.bar(
                        weather_collisions, 
                        x='weather', 
                        y='collisions',
                        title="Collisions by Weather Condition",
                        color='collisions',
                        color_continuous_scale='blues',
                        labels={'collisions': 'Number of Collisions'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No weather-collision matches in selected range")
            else:
                st.info("No weather data for selected filters")
        
        with col2:
            if 'temperature_2m' in filtered_weather.columns and len(filtered_weather) > 0:
                fig = px.histogram(
                    filtered_weather, 
                    x='temperature_2m',
                    title="Temperature Distribution (°C)",
                    nbins=30,
                    color_discrete_sequence=['orange'],
                    labels={'temperature_2m': 'Temperature (°C)'}
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No temperature data available")
    
    with tab3:
        col1, col2 = st.columns(2)
        
        with col1:
            if len(filtered_collisions) > 0:
                # Hourly pattern
                filtered_collisions['hour'] = filtered_collisions['crash_datetime'].dt.hour
                hourly_collisions = filtered_collisions.groupby('hour').size().reset_index()
                hourly_collisions.columns = ['hour', 'collisions']
                
                fig = px.line(
                    hourly_collisions, 
                    x='hour', 
                    y='collisions',
                    title="Collisions by Hour of Day",
                    markers=True,
                    labels={'hour': 'Hour (24h)', 'collisions': 'Number of Collisions'}
                )
                fig.update_traces(line_color='#FF4B4B')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No collision data for time analysis")
        
        with col2:
            if len(filtered_collisions) > 0 and 'crash_datetime' in filtered_collisions.columns:
                # Daily trend
                daily_collisions = filtered_collisions.groupby(
                    filtered_collisions['crash_datetime'].dt.date
                ).size().reset_index()
                daily_collisions.columns = ['date', 'collisions']
                
                fig = px.line(
                    daily_collisions, 
                    x='date', 
                    y='collisions',
                    title="Daily Collision Trend",
                    labels={'date': 'Date', 'collisions': 'Number of Collisions'}
                )
                fig.update_traces(line_color='#1f77b4')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data for daily trend")
    
    with tab4:
        st.subheader("📋 Raw Data Preview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Weather Data** (First 100 rows)")
            st.dataframe(
                filtered_weather.head(100),
                use_container_width=True,
                height=400
            )
        
        with col2:
            st.write("**Collisions Data** (First 100 rows)")
            st.dataframe(
                filtered_collisions.head(100),
                use_container_width=True,
                height=400
            )
    
    # ========== INSIGHTS ==========
    st.header("💡 Key Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.info("""
        **Weather Impact:**
        - Adverse weather (rain, snow, fog) increases collision risk
        - Poor visibility (<3000m) correlates with higher severity
        - Temperature extremes show varying impact patterns
        """)
        
        st.info("""
        **Temporal Patterns:**
        - Rush hours (7-10 AM, 4-7 PM) see higher collision frequency
        - Weekends show different patterns than weekdays
        - Nighttime collisions often have higher severity
        """)
    
    with col2:
        st.success("""
        **Safety Recommendations:**
        1. Increase visibility warnings during fog/rain
        2. Enhance traffic control during adverse weather
        3. Target safety campaigns for high-risk hours
        4. Borough-specific interventions based on patterns
        """)
        
        st.warning("""
        **Data Notes:**
        - Weather data is borough-level (not exact collision location)
        - Some collision records have missing location data
        - Data updates daily via automated pipeline
        """)
    
    # ========== DATA DOWNLOAD ==========
    st.header("📥 Export Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        weather_csv = filtered_weather.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📊 Download Weather Data (CSV)",
            data=weather_csv,
            file_name=f"nyc_weather_{start_filter}_{end_filter}.csv",
            mime="text/csv"
        )
    
    with col2:
        collisions_csv = filtered_collisions.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="🚗 Download Collisions Data (CSV)",
            data=collisions_csv,
            file_name=f"nyc_collisions_{start_filter}_{end_filter}.csv",
            mime="text/csv"
        )

else:
    # Show error state
    st.error("""
    ## ⚠️ No Data Available
    
    The dashboard cannot load data. Please:
    
    1. **Run the ETL pipeline:**
       ```bash
       python run_pipeline.py
       ```
    
    2. **Check that data files exist:**
       ```bash
       ls -la data/processed/
       ```
    
    3. **Files should include:**
       - `weather_master.csv`
       - `collisions_master.csv`
    
    ---
    
    **For Streamlit Cloud Deployment:**
    - Ensure files are committed to GitHub
    - GitHub Actions will create them automatically
    """)

# Footer
st.markdown("---")
st.markdown("""
**NYC Traffic Safety Analysis** | Data Sources: NYC Open Data, Open-Meteo API  
*Automated ETL Pipeline: Extract → Transform → Load → Visualize*  
*Updates daily via GitHub Actions*
""")

# Add GitHub link
st.markdown("""
<div style='text-align: center; color: #666;'>
    <a href='https://github.com/AZhuk30/WeatherImpactOnCarAccidents' target='_blank'>
        View on GitHub 🔗
    </a>
</div>
""", unsafe_allow_html=True)
