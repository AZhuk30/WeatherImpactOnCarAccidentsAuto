"""
Enhanced Data transformation for NYC Traffic Safety
Keeps ALL useful data from APIs while maintaining compatibility
FIXED: Now properly saves transformed data to CSV files
"""
import pandas as pd
import numpy as np
import logging
import os

logger = logging.getLogger(__name__)


def transform_weather_data(weather_df):
    """Transform raw weather data - KEEPS ALL USEFUL COLUMNS"""
    try:
        if weather_df.empty:
            logger.warning("⚠️ No weather data to transform")
            return pd.DataFrame()
        
        df = weather_df.copy()
        
        # Ensure date column
        if 'date' not in df.columns and 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['date'] = df['datetime'].dt.date
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Extract time features from datetime
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['hour_nyc'] = df['datetime'].dt.hour
            df['day_of_week'] = df['datetime'].dt.day_name()
            df['is_weekend'] = df['datetime'].dt.dayofweek >= 5
            df['is_rush_hour'] = df['hour_nyc'].isin([7, 8, 9, 16, 17, 18])
            df['is_night'] = (df['hour_nyc'] < 6) | (df['hour_nyc'] >= 20)
            df['month'] = df['datetime'].dt.month
            df['season'] = df['month'].map({
                12: 'WINTER', 1: 'WINTER', 2: 'WINTER',
                3: 'SPRING', 4: 'SPRING', 5: 'SPRING',
                6: 'SUMMER', 7: 'SUMMER', 8: 'SUMMER',
                9: 'FALL', 10: 'FALL', 11: 'FALL'
            })
        
        # Clean borough
        if 'borough' in df.columns:
            df['borough'] = df['borough'].str.upper()
        
        # Convert numeric columns
        for col in ['temperature_2m', 'precipitation', 'visibility', 'rain', 'showers', 'snowfall', 'wind_speed_10m']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Weather categories
        if 'precipitation' in df.columns:
            conditions = []
            choices = []
            
            if 'snowfall' in df.columns:
                conditions.append(df['snowfall'] > 0)
                choices.append('SNOW')
            if 'rain' in df.columns:
                conditions.append(df['rain'] > 0)
                choices.append('RAIN')
            
            conditions.append(True)
            choices.append('CLEAR')
            
            df['weather_category'] = np.select(conditions, choices, default='CLEAR')
            df['weather_severity'] = np.select([
                df['precipitation'] >= 10,
                df['precipitation'] >= 5,
                df['precipitation'] > 0,
                True
            ], ['SEVERE', 'MODERATE', 'LIGHT', 'LIGHT'], default='LIGHT')
        
        logger.info(f"✅ Transformed {len(df)} weather records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Weather transformation failed: {e}")
        return pd.DataFrame()


def transform_collision_data(collisions_df):
    """Transform raw collision data - KEEPS ALL USEFUL COLUMNS"""
    try:
        if collisions_df.empty:
            logger.warning("⚠️ No collision data to transform")
            return pd.DataFrame()
        
        df = collisions_df.copy()
        
        # Parse crash date
        if 'crash_date' in df.columns:
            df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
            df['date'] = df['crash_date'].dt.date
            df['hour'] = df['crash_date'].dt.hour
            df['day_of_week'] = df['crash_date'].dt.day_name()
            df['is_weekend'] = df['crash_date'].dt.dayofweek >= 5
            df['is_rush_hour'] = df['hour'].isin([7, 8, 9, 16, 17, 18])
            df['is_night'] = (df['hour'] < 6) | (df['hour'] >= 20)
            df['month'] = df['crash_date'].dt.month
            df['season'] = df['month'].map({
                12: 'WINTER', 1: 'WINTER', 2: 'WINTER',
                3: 'SPRING', 4: 'SPRING', 5: 'SPRING',
                6: 'SUMMER', 7: 'SUMMER', 8: 'SUMMER',
                9: 'FALL', 10: 'FALL', 11: 'FALL'
            })
        
        # Clean borough
        if 'borough' in df.columns:
            df['borough'] = df['borough'].astype(str).str.upper().str.strip()
            df['borough'] = df['borough'].map({
                'MANHATTAN': 'MANHATTAN',
                'BROOKLYN': 'BROOKLYN',
                'QUEENS': 'QUEENS',
                'BRONX': 'BRONX',
                'STATEN ISLAND': 'STATEN ISLAND',
                'STATEN IS': 'STATEN ISLAND'
            }).fillna('OTHER')
            df = df[df['borough'] != 'OTHER']
        
        # Convert injury/death columns
        for col in ['number_of_persons_injured', 'number_of_persons_killed',
                    'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
                    'number_of_cyclist_injured', 'number_of_cyclist_killed',
                    'number_of_motorist_injured', 'number_of_motorist_killed']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        # Severity level
        if 'number_of_persons_killed' in df.columns and 'number_of_persons_injured' in df.columns:
            df['severity_level'] = np.select([
                df['number_of_persons_killed'] > 0,
                df['number_of_persons_injured'] >= 3,
                df['number_of_persons_injured'] > 0,
                True
            ], ['FATAL', 'SEVERE', 'MODERATE', 'MINOR'], default='MINOR')
        
        # Involvement flags
        if 'number_of_pedestrians_injured' in df.columns:
            df['involved_pedestrian'] = ((df['number_of_pedestrians_injured'] > 0) | 
                                        (df.get('number_of_pedestrians_killed', 0) > 0))
        if 'number_of_cyclist_injured' in df.columns:
            df['involved_cyclist'] = ((df['number_of_cyclist_injured'] > 0) | 
                                     (df.get('number_of_cyclist_killed', 0) > 0))
        if 'vehicle_type_code2' in df.columns:
            df['involved_multiple_vehicles'] = df['vehicle_type_code2'].notna()
        
        # Location data
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        logger.info(f"✅ Transformed {len(df)} collision records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Collision transformation failed: {e}")
        return pd.DataFrame()


def run_transformation(weather_df, collisions_df):
    """
    Main transformation function
    
    FIXED: Now properly saves transformed data to CSV files in data/processed/
    """
    logger.info("🔄 Running data transformation")
    
    # Transform the data
    transformed_weather = transform_weather_data(weather_df)
    transformed_collisions = transform_collision_data(collisions_df)
    
    logger.info(f"✅ Transformation complete:")
    logger.info(f"   - Weather: {len(transformed_weather)} records")
    logger.info(f"   - Collisions: {len(transformed_collisions)} records")
    
    # CRITICAL FIX: Save transformed data to CSV files
    # This was missing and is why the files only had headers!
    try:
        # Ensure output directory exists
        os.makedirs('data/processed', exist_ok=True)
        
        # Save weather data
        if len(transformed_weather) > 0:
            weather_file = 'data/processed/weather_master.csv'
            transformed_weather.to_csv(weather_file, index=False)
            logger.info(f"💾 Saved {len(transformed_weather):,} weather records to {weather_file}")
            
            # Also create a daily summary
            if 'date' in transformed_weather.columns and 'borough' in transformed_weather.columns:
                daily_summary = transformed_weather.groupby(['date', 'borough']).agg({
                    'temperature_2m': 'mean',
                    'precipitation': 'sum',
                    'wind_speed_10m': 'mean'
                }).reset_index()
                daily_summary.to_csv('data/processed/weather_daily_summary.csv', index=False)
                logger.info(f"💾 Saved weather daily summary")
        else:
            logger.warning("⚠️  No weather data to save")
        
        # Save collision data
        if len(transformed_collisions) > 0:
            collisions_file = 'data/processed/collisions_master.csv'
            transformed_collisions.to_csv(collisions_file, index=False)
            logger.info(f"💾 Saved {len(transformed_collisions):,} collision records to {collisions_file}")
            
            # Also create a daily summary
            if 'date' in transformed_collisions.columns and 'borough' in transformed_collisions.columns:
                daily_summary = transformed_collisions.groupby(['date', 'borough']).size().reset_index(name='collision_count')
                if 'severity_level' in transformed_collisions.columns:
                    severity_summary = transformed_collisions.groupby(['date', 'borough', 'severity_level']).size().unstack(fill_value=0)
                    daily_summary = daily_summary.merge(severity_summary, on=['date', 'borough'], how='left')
                daily_summary.to_csv('data/processed/collisions_daily_summary.csv', index=False)
                logger.info(f"💾 Saved collision daily summary")
        else:
            logger.warning("⚠️  No collision data to save")
            
    except Exception as e:
        logger.error(f"❌ Failed to save CSV files: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return transformed_weather, transformed_collisions
