"""
Data transformation for NYC Traffic Safety
Transforms raw extracted data into analysis-ready format
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_weather_data(weather_df):
    """Transform raw weather data into analysis format"""
    try:
        if weather_df.empty:
            logger.warning("⚠️ No weather data to transform")
            return pd.DataFrame()
        
        # Make a copy
        df = weather_df.copy()
        
        # Ensure date column
        if 'date' not in df.columns and 'datetime' in df.columns:
            df['date'] = pd.to_datetime(df['datetime']).dt.date
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        # Ensure borough column
        if 'borough' not in df.columns:
            df['borough'] = 'UNKNOWN'
        
        # Fill missing values
        numeric_cols = ['temperature', 'precipitation', 'wind_speed']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Create condition categories
        if 'condition' not in df.columns:
            df['condition'] = 'Clear'
        
        # Select final columns
        final_cols = ['date', 'borough', 'temperature', 'precipitation', 'condition', 'wind_speed']
        available_cols = [col for col in final_cols if col in df.columns]
        
        df = df[available_cols]
        
        logger.info(f"✅ Transformed {len(df)} weather records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Weather transformation failed: {e}")
        return pd.DataFrame()

def transform_collision_data(collisions_df):
    """Transform raw collision data into analysis format"""
    try:
        if collisions_df.empty:
            logger.warning("⚠️ No collision data to transform")
            return pd.DataFrame()
        
        # Make a copy
        df = collisions_df.copy()
        
        # Create date column from crash_date
        if 'crash_date' in df.columns:
            df['date'] = pd.to_datetime(df['crash_date']).dt.date
        else:
            # Create date column if not exists
            df['date'] = pd.Timestamp.now().date()
        
        # Ensure borough column
        if 'borough' not in df.columns:
            df['borough'] = 'UNKNOWN'
        
        # Clean borough names
        df['borough'] = df['borough'].astype(str).str.upper().str.strip()
        
        # Standardize borough names
        borough_mapping = {
            'MANHATTAN': 'MANHATTAN',
            'BROOKLYN': 'BROOKLYN',
            'QUEENS': 'QUEENS',
            'BRONX': 'BRONX',
            'STATEN ISLAND': 'STATEN ISLAND',
            'STATEN IS': 'STATEN ISLAND'
        }
        
        df['borough'] = df['borough'].map(borough_mapping).fillna('OTHER')
        
        # Calculate collisions per row (usually 1)
        df['collisions'] = 1
        
        # Create severity column
        if 'persons_killed' in df.columns:
            df['persons_killed'] = pd.to_numeric(df['persons_killed'], errors='coerce').fillna(0)
        else:
            df['persons_killed'] = 0
        
        if 'persons_injured' in df.columns:
            df['persons_injured'] = pd.to_numeric(df['persons_injured'], errors='coerce').fillna(0)
        else:
            df['persons_injured'] = 0
        
        # Create weather condition if not present
        if 'weather_condition' not in df.columns:
            df['weather_condition'] = 'Clear'
        
        # Select final columns
        final_cols = ['date', 'borough', 'collisions', 'persons_injured', 
                     'persons_killed', 'weather_condition']
        available_cols = [col for col in final_cols if col in df.columns]
        
        df = df[available_cols]
        
        logger.info(f"✅ Transformed {len(df)} collision records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Collision transformation failed: {e}")
        return pd.DataFrame()
