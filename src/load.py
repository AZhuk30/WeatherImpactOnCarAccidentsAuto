"""
Compatible data loader for Streamlit app
Works with both old and new column names
"""

import streamlit as st
import pandas as pd


@st.cache_data(ttl=3600)
def load_collision_data():
    """Load collision data with column name compatibility"""
    try:
        df = pd.read_csv('data/processed/collisions_master.csv')
        
        # Convert dates
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        if 'crash_date' in df.columns:
            df['crash_date'] = pd.to_datetime(df['crash_date'])
            if 'date' not in df.columns:
                df['date'] = df['crash_date']
        
        # CREATE COMPATIBILITY COLUMNS - Map new names to old names
        column_mapping = {
            'number_of_persons_injured': 'persons_injured',
            'number_of_persons_killed': 'persons_killed',
            'number_of_pedestrians_injured': 'pedestrians_injured',
            'number_of_pedestrians_killed': 'pedestrians_killed',
            'number_of_cyclist_injured': 'cyclists_injured',
            'number_of_cyclist_killed': 'cyclists_killed',
        }
        
        # Add old column names for backward compatibility
        for new_col, old_col in column_mapping.items():
            if new_col in df.columns and old_col not in df.columns:
                df[old_col] = df[new_col]
        
        # Ensure numeric columns are proper type
        numeric_cols = ['persons_injured', 'persons_killed', 
                       'pedestrians_injured', 'pedestrians_killed',
                       'cyclists_injured', 'cyclists_killed']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
        
    except FileNotFoundError:
        st.error("⚠️ Collision data not found")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading collision data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_weather_data():
    """Load weather data with column name compatibility"""
    try:
        df = pd.read_csv('data/processed/weather_master.csv')
        
        # Convert dates
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            if 'date' not in df.columns:
                df['date'] = df['datetime'].dt.date
        
        # CREATE COMPATIBILITY COLUMNS - Map new names to old names
        column_mapping = {
            'temperature_2m': 'temperature',
            'weather_category': 'condition',
            'weather_severity': 'severity',
        }
        
        # Add old column names for backward compatibility
        for new_col, old_col in column_mapping.items():
            if new_col in df.columns and old_col not in df.columns:
                df[old_col] = df[new_col]
        
        # Ensure numeric columns
        numeric_cols = ['temperature', 'precipitation', 'wind_speed_10m', 'visibility']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
        
    except FileNotFoundError:
        st.error("⚠️ Weather data not found")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading weather data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def merge_weather_collision_data():
    """Merge weather and collision data on date and borough"""
    collisions = load_collision_data()
    weather = load_weather_data()
    
    if collisions.empty or weather.empty:
        return pd.DataFrame()
    
    # Aggregate weather by date and borough (hourly → daily)
    if 'datetime' in weather.columns:
        weather_daily = weather.groupby(['date', 'borough']).agg({
            'temperature': 'mean',
            'precipitation': 'sum',
            'wind_speed_10m': 'mean',
            'condition': lambda x: x.mode()[0] if len(x) > 0 else 'Clear',
        }).reset_index()
    else:
        weather_daily = weather
    
    # Merge on date and borough
    merged = pd.merge(
        collisions,
        weather_daily,
        on=['date', 'borough'],
        how='left',
        suffixes=('', '_weather')
    )
    
    return merged


def get_data_summary():
    """Get summary statistics for the dashboard"""
    collisions = load_collision_data()
    weather = load_weather_data()
    
    if collisions.empty:
        return {}
    
    summary = {
        'total_collisions': len(collisions),
        'total_injured': collisions['persons_injured'].sum() if 'persons_injured' in collisions.columns else 0,
        'total_killed': collisions['persons_killed'].sum() if 'persons_killed' in collisions.columns else 0,
        'date_start': collisions['date'].min() if 'date' in collisions.columns else None,
        'date_end': collisions['date'].max() if 'date' in collisions.columns else None,
        'boroughs': sorted(collisions['borough'].unique().tolist()) if 'borough' in collisions.columns else [],
        'has_severity': 'severity_level' in collisions.columns,
        'has_weather': not weather.empty,
        'has_temperature': 'temperature' in weather.columns,
        'has_condition': 'condition' in weather.columns,
    }
    
    # Calculate date range
    if summary['date_start'] and summary['date_end']:
        days = (summary['date_end'] - summary['date_start']).days + 1
        summary['days'] = days
        summary['avg_daily_collisions'] = summary['total_collisions'] / days if days > 0 else 0
    
    return summary


# Example usage in app.py:
if __name__ == "__main__":
    st.set_page_config(page_title="NYC Traffic Safety", layout="wide")
    
    st.title("🚗 NYC Traffic Safety - Weather Impact Analysis")
    
    # Load data
    summary = get_data_summary()
    collisions = load_collision_data()
    weather = load_weather_data()
    merged = merge_weather_collision_data()
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Collisions", f"{summary.get('total_collisions', 0):,}")
    with col2:
        st.metric("Total Injuries", f"{summary.get('total_injured', 0):,}")
    with col3:
        st.metric("Total Fatalities", f"{summary.get('total_killed', 0):,}")
    with col4:
        st.metric("Avg Daily Collisions", f"{summary.get('avg_daily_collisions', 0):.1f}")
    
    # Show what data is available
    st.sidebar.header("📊 Data Status")
    st.sidebar.write(f"✅ Collisions: {len(collisions):,} records")
    st.sidebar.write(f"✅ Weather: {len(weather):,} records")
    st.sidebar.write(f"✅ Has severity data: {summary.get('has_severity', False)}")
    st.sidebar.write(f"✅ Has weather data: {summary.get('has_weather', False)}")
    st.sidebar.write(f"✅ Has temperature: {summary.get('has_temperature', False)}")
    
    # Show available columns for debugging
    with st.expander("🔍 Debug: Available Columns"):
        st.write("**Collision columns:**", collisions.columns.tolist())
        st.write("**Weather columns:**", weather.columns.tolist())
