"""
Data transformation and cleaning with ACCUMULATION support
Weather + NYC Collisions - NOW STORES HISTORICAL DATA
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

from src.config import BOROUGHS, PROCESSED_DATA_DIR

logger = logging.getLogger(__name__)


# =========================================================
# WEATHER TRANSFORMER
# =========================================================

class WeatherTransformer:
    """Transform and clean weather data"""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Transforming {len(df)} weather records")

        df = df.copy()

        # Normalize column names
        df.columns = (
            df.columns
              .str.lower()
              .str.strip()
              .str.replace(" ", "_")
        )

        # Datetime handling
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True, errors='coerce')
        df['datetime'] = df['datetime'].dt.tz_convert('America/New_York')

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Numeric cleanup
        numeric_cols = [
            'temperature_2m', 'precipitation', 'visibility',
            'rain', 'showers', 'snowfall', 'wind_speed_10m'
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        if 'temperature_2m' in df.columns:
            df['temperature_2m'] = df['temperature_2m'].round(2)

        if 'wind_speed_10m' in df.columns:
            df['wind_speed_10m'] = df['wind_speed_10m'].round(2)

        if 'visibility' in df.columns:
            df['visibility'] = (df['visibility'] / 100).round() * 100

        # Time-based features
        df['hour_nyc'] = df['datetime'].dt.hour
        df['day_of_week'] = df['datetime'].dt.day_name()
        df['is_weekend'] = df['datetime'].dt.dayofweek >= 5
        df['is_rush_hour'] = df['hour_nyc'].isin([7, 8, 9, 16, 17, 18, 19])
        df['is_night'] = (df['hour_nyc'] >= 20) | (df['hour_nyc'] < 6)

        df['month'] = df['datetime'].dt.month
        df['season'] = df['month'].apply(self._get_season)

        # Weather categorization
        df['weather_category'] = df.apply(self._categorize_weather, axis=1)
        df['weather_severity'] = df.apply(self._assess_severity, axis=1)

        # Borough cleanup
        if 'borough' in df.columns:
            df['borough'] = df['borough'].str.upper().str.strip()

        # Deduplication
        df = df.drop_duplicates(subset=['borough', 'datetime'])

        logger.info(f"Weather transformation complete: {len(df)} records")
        return df

    @staticmethod
    def _get_season(month: int) -> str:
        if month in [12, 1, 2]:
            return 'WINTER'
        elif month in [3, 4, 5]:
            return 'SPRING'
        elif month in [6, 7, 8]:
            return 'SUMMER'
        return 'FALL'

    @staticmethod
    def _categorize_weather(row) -> str:
        if row.get('snowfall', 0) > 0:
            return 'SNOW'

        rain_total = (
            row.get('rain', 0)
            + row.get('showers', 0)
            + row.get('precipitation', 0)
        )

        if rain_total > 0:
            return 'RAIN'

        if row.get('visibility', 10000) < 5000:
            return 'FOG'

        if row.get('wind_speed_10m', 0) > 30:
            return 'WIND'

        return 'CLEAR'

    @staticmethod
    def _assess_severity(row) -> str:
        if row.get('snowfall', 0) > 5:
            return 'HEAVY'

        rain_total = (
            row.get('rain', 0)
            + row.get('showers', 0)
            + row.get('precipitation', 0)
        )

        if rain_total > 10:
            return 'HEAVY'
        if rain_total > 5:
            return 'MODERATE'

        if row.get('visibility', 10000) < 1000:
            return 'SEVERE'
        if row.get('visibility', 10000) < 3000:
            return 'MODERATE'

        if row.get('wind_speed_10m', 0) > 50:
            return 'SEVERE'
        if row.get('wind_speed_10m', 0) > 30:
            return 'MODERATE'

        return 'LIGHT'


# =========================================================
# COLLISIONS TRANSFORMER
# =========================================================

class CollisionsTransformer:
    """Transform and clean NYC collisions data"""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Transforming {len(df)} collision records")

        df = df.copy()

        # Normalize column names
        df.columns = (
            df.columns
              .str.lower()
              .str.strip()
              .str.replace(" ", "_")
        )

        # Rename to standard schema
        column_mapping = {
            'number_of_persons_injured': 'persons_injured',
            'number_of_persons_killed': 'persons_killed',
            'number_of_pedestrians_injured': 'pedestrians_injured',
            'number_of_pedestrians_killed': 'pedestrians_killed',
            'number_of_cyclist_injured': 'cyclists_injured',
            'number_of_cyclist_killed': 'cyclists_killed',
            'number_of_motorist_injured': 'motorists_injured',
            'number_of_motorist_killed': 'motorists_killed',
        }

        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

        # Datetime parsing
        df['crash_datetime'] = self._parse_crash_datetime(df)

        # Borough cleanup
        if 'borough' in df.columns:
            df['borough'] = df['borough'].fillna('UNKNOWN').str.upper().str.strip()
            valid = list(BOROUGHS.keys()) + ['UNKNOWN']
            df = df[df['borough'].isin(valid)]

        # Injury columns
        injury_cols = [
            'persons_injured', 'persons_killed',
            'pedestrians_injured', 'pedestrians_killed',
            'cyclists_injured', 'cyclists_killed',
            'motorists_injured', 'motorists_killed'
        ]

        for col in injury_cols:
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Coordinates
        for col in ['latitude', 'longitude']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(9)

        # Drop invalid records
        df = df.dropna(subset=['collision_id', 'crash_datetime'])
        df = df.drop_duplicates(subset=['collision_id'])

        # Derived features
        df['has_injuries'] = df['persons_injured'] > 0
        df['has_fatalities'] = df['persons_killed'] > 0

        df['total_involved'] = (
            df['persons_injured']
            + df['persons_killed']
            + df['pedestrians_injured']
            + df['pedestrians_killed']
            + df['cyclists_injured']
            + df['cyclists_killed']
            + df['motorists_injured']
            + df['motorists_killed']
        )

        df['severity_level'] = df.apply(self._determine_severity, axis=1)

        logger.info(f"Collisions transformation complete: {len(df)} records")
        return df

    @staticmethod
    def _parse_crash_datetime(df: pd.DataFrame) -> pd.Series:
        datetimes = []

        for _, row in df.iterrows():
            try:
                date_str = str(row.get('crash_date', '')).split('T')[0]
                time_str = str(row.get('crash_time', '00:00')).strip()

                if ':' in time_str and len(time_str.split(':')[0]) == 1:
                    time_str = f"0{time_str}"

                dt = pd.to_datetime(f"{date_str} {time_str}", errors='coerce')
                datetimes.append(dt)
            except Exception:
                datetimes.append(pd.NaT)

        return pd.Series(datetimes, index=df.index)

    @staticmethod
    def _determine_severity(row) -> str:
        if row['persons_killed'] > 0:
            return 'FATAL'
        if row['persons_injured'] >= 3:
            return 'SEVERE'
        if row['persons_injured'] > 0:
            return 'MODERATE'
        if row['total_involved'] > 0:
            return 'MINOR'
        return 'NONE'


# =========================================================
# PIPELINE ENTRY WITH ACCUMULATION
# =========================================================

def run_transformation(weather_df: pd.DataFrame, collisions_df: pd.DataFrame):
    """
    Run full transformation process with DATA ACCUMULATION
    
    This creates and maintains master CSV files that grow over time
    """
    logger.info("=" * 50)
    logger.info("STARTING DATA TRANSFORMATION (WITH ACCUMULATION)")
    logger.info("=" * 50)

    # Transform new data
    weather_clean = WeatherTransformer().transform(weather_df)
    collisions_clean = CollisionsTransformer().transform(collisions_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Define master file paths (persistent across runs)
    weather_master = PROCESSED_DATA_DIR / "weather_master.csv"
    collisions_master = PROCESSED_DATA_DIR / "collisions_master.csv"
    
    # ===== ACCUMULATE WEATHER DATA =====
    if weather_master.exists():
        logger.info("📂 Loading existing weather master file...")
        existing_weather = pd.read_csv(weather_master)
        
        # Ensure datetime column is parsed
        existing_weather['datetime'] = pd.to_datetime(existing_weather['datetime'])
        
        # Append new data
        weather_combined = pd.concat([existing_weather, weather_clean], ignore_index=True)
        
        # Remove duplicates (keep latest version)
        weather_combined = weather_combined.drop_duplicates(
            subset=['borough', 'datetime'], 
            keep='last'
        )
        
        # Sort by datetime
        weather_combined = weather_combined.sort_values('datetime').reset_index(drop=True)
        
        new_records = len(weather_combined) - len(existing_weather)
        logger.info(f"   ✅ Added {new_records} new weather records")
        logger.info(f"   📊 Total weather records: {len(weather_combined):,}")
        
        # Show date range
        min_date = weather_combined['datetime'].min()
        max_date = weather_combined['datetime'].max()
        logger.info(f"   📅 Date range: {min_date.date()} to {max_date.date()}")
        
    else:
        logger.info("📂 Creating new weather master file...")
        weather_combined = weather_clean
        logger.info(f"   ✅ Initialized with {len(weather_combined):,} records")
    
    # ===== ACCUMULATE COLLISION DATA =====
    if collisions_master.exists():
        logger.info("📂 Loading existing collisions master file...")
        existing_collisions = pd.read_csv(collisions_master)
        
        # Ensure datetime column is parsed
        existing_collisions['crash_datetime'] = pd.to_datetime(existing_collisions['crash_datetime'])
        
        # Append new data
        collisions_combined = pd.concat([existing_collisions, collisions_clean], ignore_index=True)
        
        # Remove duplicates (keep latest version)
        collisions_combined = collisions_combined.drop_duplicates(
            subset=['collision_id'], 
            keep='last'
        )
        
        # Sort by crash datetime
        collisions_combined = collisions_combined.sort_values('crash_datetime').reset_index(drop=True)
        
        new_records = len(collisions_combined) - len(existing_collisions)
        logger.info(f"   ✅ Added {new_records} new collision records")
        logger.info(f"   📊 Total collision records: {len(collisions_combined):,}")
        
        # Show date range
        min_date = collisions_combined['crash_datetime'].min()
        max_date = collisions_combined['crash_datetime'].max()
        logger.info(f"   📅 Date range: {min_date.date()} to {max_date.date()}")
        
    else:
        logger.info("📂 Creating new collisions master file...")
        collisions_combined = collisions_clean
        logger.info(f"   ✅ Initialized with {len(collisions_combined):,} records")
    
    # ===== SAVE MASTER FILES =====
    logger.info("\n💾 Saving master files...")
    weather_combined.to_csv(weather_master, index=False)
    collisions_combined.to_csv(collisions_master, index=False)
    logger.info(f"   ✅ Weather master: {weather_master}")
    logger.info(f"   ✅ Collisions master: {collisions_master}")
    
    # ===== SAVE TIMESTAMPED BACKUPS =====
    logger.info("\n💾 Saving timestamped backups...")
    weather_backup = PROCESSED_DATA_DIR / f"weather_{timestamp}.csv"
    collisions_backup = PROCESSED_DATA_DIR / f"collisions_{timestamp}.csv"
    
    weather_clean.to_csv(weather_backup, index=False)
    collisions_clean.to_csv(collisions_backup, index=False)
    logger.info(f"   ✅ Weather backup: {weather_backup}")
    logger.info(f"   ✅ Collisions backup: {collisions_backup}")
    
    # ===== SUMMARY =====
    logger.info("\n" + "=" * 50)
    logger.info("TRANSFORMATION COMPLETED SUCCESSFULLY")
    logger.info("=" * 50)
    logger.info(f"Master files contain ALL historical data")
    logger.info(f"  Weather: {len(weather_combined):,} total records")
    logger.info(f"  Collisions: {len(collisions_combined):,} total records")
    logger.info("=" * 50)

    # Return the FULL combined datasets
    return weather_combined, collisions_combined
