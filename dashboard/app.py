"""
Streamlit Dashboard for NYC Traffic Safety Analysis
UPDATED VERSION - Works with enhanced API data
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, date
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
@st.cache_data(ttl=3600)
def load_data():
    """
    Load master CSV files with column compatibility
    """
    try:
        # Check if files exist
        if not os.path.exists("data/processed/weather_master.csv"):
            st.error("weather_master.csv not found")
            return None, None
            
        if not os.path.exists("data/processed/collisions_master.csv"):
            st.error("collisions_master.csv not found")
            return None, None
        
        # Load files
        weather_df = pd.read_csv("data/processed/weather_master.csv")
        collisions_df = pd.read_csv("data/processed/collisions_master.csv")
        
        # Convert date columns
        if 'date' in weather_df.columns:
            weather_df['date'] = pd.to_datetime(weather_df['date'])
        
        if 'date' in collisions_df.columns:
            collisions_df['date'] = pd.to_datetime(collisions_df['date'])
        elif 'crash_date' in collisions_df.columns:
            collisions_df['date'] = pd.to_datetime(collisions_df['crash_date'])
        
        # ========== COLUMN COMPATIBILITY MAPPING ==========
        # Map new API column names to old app column names
        
        # Weather columns
        if 'temperature_2m' in weather_df.columns:
            weather_df['temperature'] = weather_df['temperature_2m']
        if 'weather_category' in weather_df.columns:
            weather_df['condition'] = weather_df['weather_category']
        
        # Collision columns
        if 'number_of_persons_injured' in collisions_df.columns:
            collisions_df['persons_injured'] = collisions_df['number_of_persons_injured']
        if 'number_of_persons_killed' in collisions_df.columns:
            collisions_df['persons_killed'] = collisions_df['number_of_persons_killed']
        
        # Add weather_condition to collisions if it doesn't exist
        # We can derive it from the weather category if available
        if 'weather_condition' not in collisions_df.columns:
            # For now, set a default - this could be enhanced with actual weather join
            collisions_df['weather_condition'] = 'Clear'
        
        return weather_df, collisions_df
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None, None

# Load data
with st.spinner('Loading data...'):
    weather_df, collisions_df = load_data()

if weather_df is not None and collisions_df is not None and len(weather_df) > 0 and len(collisions_df) > 0:
    
    # ========== SIDEBAR FILTERS ==========
    st.sidebar.header("🔍 Filters")
    
    # Borough filter
    boroughs = ['ALL'] + sorted(collisions_df['borough'].dropna().unique().tolist())
    selected_borough = st.sidebar.selectbox("Select Borough", boroughs)
    
    # Date range filter
    if 'date' in collisions_df.columns:
        min_date = collisions_df['date'].min().date()
        max_date = collisions_df['date'].max().date()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range[0]
    
    # Weather condition filter
    if 'condition' in weather_df.columns:
        weather_conditions = ['ALL'] + sorted(weather_df['condition'].dropna().unique().tolist())
        selected_weather = st.sidebar.selectbox("Weather Condition", weather_conditions)
    
    # Apply filters
    filtered_collisions = collisions_df.copy()
    filtered_weather = weather_df.copy()
    
    # Filter by borough
    if selected_borough != 'ALL':
        filtered_collisions = filtered_collisions[filtered_collisions['borough'] == selected_borough]
        filtered_weather = filtered_weather[filtered_weather['borough'] == selected_borough]
    
    # Filter by date range
    if 'date' in filtered_collisions.columns:
        filtered_collisions = filtered_collisions[
            (filtered_collisions['date'].dt.date >= start_date) &
            (filtered_collisions['date'].dt.date <= end_date)
        ]
    
    if 'date' in filtered_weather.columns:
        filtered_weather = filtered_weather[
            (filtered_weather['date'].dt.date >= start_date) &
            (filtered_weather['date'].dt.date <= end_date)
        ]
    
    # Filter by weather
    if 'selected_weather' in locals() and selected_weather != 'ALL':
        filtered_weather = filtered_weather[filtered_weather['condition'] == selected_weather]
    
    # ========== DASHBOARD METRICS ==========
    st.header("📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_collisions = len(filtered_collisions)
        st.metric("Total Collisions", f"{total_collisions:,}")
    
    with col2:
        if 'persons_injured' in filtered_collisions.columns:
            total_injuries = int(filtered_collisions['persons_injured'].sum())
            st.metric("Total Injuries", f"{total_injuries:,}")
        else:
            st.metric("Total Injuries", "N/A")
    
    with col3:
        if 'persons_killed' in filtered_collisions.columns:
            total_fatalities = int(filtered_collisions['persons_killed'].sum())
            st.metric("Total Fatalities", total_fatalities)
        else:
            st.metric("Total Fatalities", "N/A")
    
    with col4:
        # Calculate collisions per day
        if len(filtered_collisions) > 0 and 'date' in filtered_collisions.columns:
            days_count = (filtered_collisions['date'].max() - filtered_collisions['date'].min()).days + 1
            if days_count > 0:
                daily_avg = total_collisions / days_count
                st.metric("Avg Daily Collisions", f"{daily_avg:.1f}")
            else:
                st.metric("Avg Daily", "N/A")
        else:
            st.metric("Avg Daily", "N/A")
    
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
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No collision data for selected filters")
        
        with col2:
            # Use severity_level if available, otherwise calculate from injuries
            if 'severity_level' in filtered_collisions.columns:
                severity_counts = filtered_collisions['severity_level'].value_counts().reset_index()
                severity_counts.columns = ['severity', 'count']
            elif 'persons_injured' in filtered_collisions.columns and 'persons_killed' in filtered_collisions.columns:
                def assign_severity(row):
                    if row['persons_killed'] > 0:
                        return 'FATAL'
                    elif row['persons_injured'] >= 3:
                        return 'SEVERE'
                    elif row['persons_injured'] > 0:
                        return 'MODERATE'
                    else:
                        return 'MINOR'
                
                filtered_collisions['severity'] = filtered_collisions.apply(assign_severity, axis=1)
                severity_counts = filtered_collisions['severity'].value_counts().reset_index()
                severity_counts.columns = ['severity', 'count']
            else:
                severity_counts = None
            
            if severity_counts is not None and len(severity_counts) > 0:
                fig = px.pie(
                    severity_counts, 
                    values='count', 
                    names='severity',
                    title="Collision Severity Distribution",
                    hole=0.3,
                    color_discrete_sequence=px.colors.sequential.Reds_r
                )
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No severity data available")
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            # Weather impact on collisions
            if 'condition' in filtered_weather.columns and len(filtered_weather) > 0:
                # Aggregate collisions by date and borough
                collision_daily = filtered_collisions.groupby(['date', 'borough']).size().reset_index(name='collisions')
                
                # Aggregate weather by date and borough
                weather_daily = filtered_weather.groupby(['date', 'borough']).agg({
                    'condition': lambda x: x.mode()[0] if len(x) > 0 else 'Clear'
                }).reset_index()
                
                # Merge them
                merged = pd.merge(collision_daily, weather_daily, on=['date', 'borough'], how='left')
                
                # Count collisions by weather condition
                weather_collisions = merged.groupby('condition')['collisions'].sum().reset_index()
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
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No weather condition data available")
        
        with col2:
            if 'temperature' in filtered_weather.columns and len(filtered_weather) > 0:
                # Convert Celsius to Fahrenheit if needed
                temp_col = filtered_weather['temperature'].copy()
                if temp_col.mean() < 50:  # Likely Celsius
                    temp_col = (temp_col * 9/5) + 32
                
                fig = px.histogram(
                    temp_col, 
                    title="Temperature Distribution",
                    nbins=20,
                    color_discrete_sequence=['orange'],
                    labels={'value': 'Temperature (°F)', 'count': 'Frequency'}
                )
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No temperature data available")
    
    with tab3:
        col1, col2 = st.columns(2)
        
        with col1:
            if len(filtered_collisions) > 0 and 'date' in filtered_collisions.columns:
                # Daily trend
                daily_collisions = filtered_collisions.groupby(
                    filtered_collisions['date'].dt.date
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
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No data for daily trend")
        
        with col2:
            if 'condition' in filtered_weather.columns and len(filtered_weather) > 0:
                # Weather frequency
                weather_counts = filtered_weather['condition'].value_counts().reset_index()
                weather_counts.columns = ['condition', 'count']
                
                fig = px.pie(
                    weather_counts,
                    values='count',
                    names='condition',
                    title="Weather Condition Frequency",
                    hole=0.3
                )
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No weather data available")
    
    with tab4:
        st.subheader("📋 Data Preview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Weather Data**")
            # Show relevant columns
            display_cols = ['date', 'borough', 'temperature', 'precipitation', 'condition']
            available_cols = [col for col in display_cols if col in filtered_weather.columns]
            
            st.dataframe(
                filtered_weather[available_cols].head(50),
                use_container_width=True,
                height=300
            )
            
            # Show weather stats
            if 'temperature' in filtered_weather.columns:
                st.write("**Weather Statistics:**")
                temp_avg = filtered_weather['temperature'].mean()
                # Convert to Fahrenheit if Celsius
                if temp_avg < 50:
                    temp_avg = (temp_avg * 9/5) + 32
                st.write(f"- Avg Temperature: {temp_avg:.1f}°F")
                
                if 'precipitation' in filtered_weather.columns:
                    st.write(f"- Avg Precipitation: {filtered_weather['precipitation'].mean():.2f} mm")
        
        with col2:
            st.write("**Collisions Data**")
            # Show relevant columns
            display_cols = ['date', 'borough', 'persons_injured', 'persons_killed', 'severity_level']
            available_cols = [col for col in display_cols if col in filtered_collisions.columns]
            
            st.dataframe(
                filtered_collisions[available_cols].head(50),
                use_container_width=True,
                height=300
            )
            
            # Show collision stats
            st.write("**Collision Statistics:**")
            st.write(f"- Total Collisions: {len(filtered_collisions):,}")
            if 'persons_injured' in filtered_collisions.columns:
                injury_rate = (filtered_collisions['persons_injured'] > 0).sum() / len(filtered_collisions) * 100
                st.write(f"- Injury Rate: {injury_rate:.1f}%")
            if 'persons_killed' in filtered_collisions.columns:
                fatality_rate = (filtered_collisions['persons_killed'] > 0).sum() / len(filtered_collisions) * 100
                st.write(f"- Fatality Rate: {fatality_rate:.1f}%")
    
    # ========== INSIGHTS ==========
    st.header("💡 Insights & Summary")
    
    insights = []
    
    if len(filtered_collisions) > 0:
        # Borough with most collisions
        if 'borough' in filtered_collisions.columns:
            top_borough = filtered_collisions['borough'].value_counts().index[0]
            insights.append(f"**{top_borough}** has the most collisions in the selected period")
        
        # Weather impact
        if 'condition' in filtered_weather.columns and len(filtered_weather) > 0:
            # Count weather conditions
            weather_counts = filtered_weather['condition'].value_counts()
            if len(weather_counts) > 0:
                top_condition = weather_counts.index[0]
                percentage = (weather_counts.iloc[0] / len(filtered_weather)) * 100
                insights.append(f"**{percentage:.1f}%** of the time had **{top_condition}** weather conditions")
        
        # Time pattern
        if 'date' in filtered_collisions.columns:
            days = (filtered_collisions['date'].max() - filtered_collisions['date'].min()).days + 1
            if days > 0:
                insights.append(f"Average of **{len(filtered_collisions)/days:.1f} collisions per day** in the selected period")
        
        # Injury statistics
        if 'persons_injured' in filtered_collisions.columns:
            total_injured = int(filtered_collisions['persons_injured'].sum())
            if total_injured > 0:
                insights.append(f"**{total_injured:,} people injured** in total during this period")
    
    if insights:
        for insight in insights:
            st.info(insight)
    else:
        st.info("Select filters and load data to see insights")
    
    # ========== DATA DOWNLOAD ==========
    st.header("📥 Export Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if len(filtered_weather) > 0:
            weather_csv = filtered_weather.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📊 Download Weather Data (CSV)",
                data=weather_csv,
                file_name=f"nyc_weather_{start_date}_{end_date}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No weather data to download")
    
    with col2:
        if len(filtered_collisions) > 0:
            collisions_csv = filtered_collisions.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="🚗 Download Collisions Data (CSV)",
                data=collisions_csv,
                file_name=f"nyc_collisions_{start_date}_{end_date}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No collision data to download")

else:
    # Show loading/error state
    if weather_df is None or collisions_df is None:
        st.error("""
        ## ⚠️ Data Files Not Found
        
        Master data files are missing. Please:
        
        1. **Run the ETL pipeline:**
           ```bash
           python run_pipeline.py
           ```
        
        2. **Check that files exist:**
           ```bash
           ls -la data/processed/
           ```
        
        Files should include:
        - `weather_master.csv`
        - `collisions_master.csv`
        """)
    elif len(weather_df) == 0 or len(collisions_df) == 0:
        st.warning("""
        ## 📭 Data Files Are Empty
        
        The data files exist but contain no records. Please:
        
        1. **Run the pipeline to generate data:**
           ```bash
           python run_pipeline.py --days 30
           ```
        
        2. **Check GitHub Actions ran successfully**
        """)
    else:
        st.warning("Loading data...")

# Footer
st.markdown("---")
st.markdown("""
**NYC Traffic Safety Analysis** | Data Sources: NYC Open Data  
*Automated ETL Pipeline: Extract → Transform → Load → Visualize*  
*Updates daily via GitHub Actions*
""")
