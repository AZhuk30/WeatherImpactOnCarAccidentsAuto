"""
Streamlit Dashboard for NYC Traffic Safety Analysis
DIAGNOSTIC VERSION - Shows what files are found
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

# DIAGNOSTIC SECTION
st.sidebar.markdown("### 🔍 Diagnostic Info")

# Check what files exist
st.sidebar.write("**Checking file system...**")

# Show current directory
current_dir = os.getcwd()
st.sidebar.write(f"Current directory: `{current_dir}`")

# Check if data directory exists
if os.path.exists("data"):
    st.sidebar.success("✅ `data/` folder exists")
    
    if os.path.exists("data/processed"):
        st.sidebar.success("✅ `data/processed/` folder exists")
        
        # List files in data/processed
        try:
            files = os.listdir("data/processed")
            st.sidebar.write(f"**Files found ({len(files)}):**")
            for f in files:
                st.sidebar.write(f"  - `{f}`")
        except Exception as e:
            st.sidebar.error(f"Error listing files: {e}")
    else:
        st.sidebar.error("❌ `data/processed/` folder NOT found")
        st.sidebar.write("**All directories:**")
        try:
            for item in os.listdir("data"):
                st.sidebar.write(f"  - data/{item}")
        except:
            pass
else:
    st.sidebar.error("❌ `data/` folder NOT found")
    st.sidebar.write("**Root directories:**")
    try:
        root_items = os.listdir(".")
        for item in root_items[:10]:  # Show first 10
            st.sidebar.write(f"  - {item}")
    except:
        pass

# Load data function
@st.cache_data(ttl=3600)
def load_data():
    """Load master CSV files with detailed error reporting"""
    
    weather_path = "data/processed/weather_master.csv"
    collisions_path = "data/processed/collisions_master.csv"
    
    # Check weather file
    if not os.path.exists(weather_path):
        st.error(f"❌ Weather file not found at: `{weather_path}`")
        return None, None
    
    # Check collisions file
    if not os.path.exists(collisions_path):
        st.error(f"❌ Collisions file not found at: `{collisions_path}`")
        return None, None
    
    try:
        # Load weather data
        st.info(f"📂 Loading weather data from `{weather_path}`...")
        weather_df = pd.read_csv(weather_path)
        st.success(f"✅ Weather data loaded: {len(weather_df)} rows, {len(weather_df.columns)} columns")
        st.write(f"**Weather columns:** {', '.join(weather_df.columns.tolist())}")
        
        # Load collisions data
        st.info(f"📂 Loading collisions data from `{collisions_path}`...")
        collisions_df = pd.read_csv(collisions_path)
        st.success(f"✅ Collisions data loaded: {len(collisions_df)} rows, {len(collisions_df.columns)} columns")
        st.write(f"**Collision columns:** {', '.join(collisions_df.columns.tolist())}")
        
        # Convert date columns
        if 'date' in weather_df.columns:
            weather_df['date'] = pd.to_datetime(weather_df['date'])
        
        if 'date' in collisions_df.columns:
            collisions_df['date'] = pd.to_datetime(collisions_df['date'])
        elif 'crash_date' in collisions_df.columns:
            collisions_df['date'] = pd.to_datetime(collisions_df['crash_date'])
        
        return weather_df, collisions_df
        
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None, None

# Main section
st.header("📊 Data Loading Status")

# Try to load data
weather_df, collisions_df = load_data()

if weather_df is not None and collisions_df is not None:
    st.success("🎉 **Data loaded successfully!**")
    
    # Show preview
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Weather Data Preview")
        st.dataframe(weather_df.head(), use_container_width=True)
    
    with col2:
        st.subheader("Collisions Data Preview")
        st.dataframe(collisions_df.head(), use_container_width=True)
    
    # Basic stats
    st.header("📈 Quick Statistics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Weather Records", len(weather_df))
    
    with col2:
        st.metric("Collision Records", len(collisions_df))
    
    with col3:
        if 'borough' in weather_df.columns:
            st.metric("Boroughs (Weather)", weather_df['borough'].nunique())
        else:
            st.metric("Boroughs (Weather)", "N/A")
    
    with col4:
        if 'borough' in collisions_df.columns:
            st.metric("Boroughs (Collisions)", collisions_df['borough'].nunique())
        else:
            st.metric("Boroughs (Collisions)", "N/A")
    
    # Show full dashboard link
    st.info("✅ **Data is loading correctly!** You can now replace this diagnostic app with the full dashboard app.py")
    
else:
    st.error("⚠️ **Data files could not be loaded.**")
    st.write("**Troubleshooting steps:**")
    st.write("1. Check the sidebar for file system diagnostics")
    st.write("2. Verify files are committed to GitHub in `data/processed/` folder")
    st.write("3. Check file names are exactly: `weather_master.csv` and `collisions_master.csv`")
    st.write("4. Verify files are not empty")

# Footer
st.markdown("---")
st.markdown("**Diagnostic Mode** | This app checks file system and data loading")
