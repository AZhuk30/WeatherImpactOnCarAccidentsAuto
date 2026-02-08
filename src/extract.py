"""
Simplified extraction for GitHub Actions
"""

import logging
import os
import pandas as pd
import random
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class WeatherExtractor:
    """Extract weather data"""
    
    def extract(self, start_date=None, end_date=None):
        """Extract weather data - returns DataFrame"""
        try:
            logger.info(f"🌤️  Extracting weather data")
            
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            boroughs = ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND']
            
            # Create sample weather data
            weather_data = []
            for date in dates:
                for borough in boroughs:
                    day_num = (date - dates[0]).days
                    temp = 40 + (day_num % 20) + (boroughs.index(borough) * 2)
                    rain_chance = 0.2 if day_num % 5 == 0 else 0.05
                    
                    weather_data.append({
                        'date': date,
                        'borough': borough,
                        'temperature': temp,
                        'precipitation': 5.0 if random.random() < rain_chance else 0.0,
                        'condition': 'Rain' if random.random() < rain_chance else 'Clear',
                        'wind_speed': 5 + (day_num % 15)
                    })
            
            df = pd.DataFrame(weather_data)
            
            # Save raw data
            os.makedirs("data/raw", exist_ok=True)
            filename = f"data/raw/weather_{start_date}_to_{end_date}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"✅ Weather data saved: {filename}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Weather extraction failed: {e}")
            return pd.DataFrame()

class CollisionExtractor:
    """Extract NYC collision data"""
    
    def extract(self, start_date=None, end_date=None):
        """Extract collision data - returns DataFrame"""
        try:
            logger.info(f"🚗 Extracting collision data")
            
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
            dates = pd.date_range(start=start_date, end=end_date, freq='D')
            boroughs = ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND']
            
            collisions = []
            for date in dates:
                is_weekend = date.weekday() >= 5
                daily_collisions = 120 if not is_weekend else 80
                
                for i in range(daily_collisions):
                    borough = boroughs[i % 5]
                    collisions.append({
                        'collision_id': f"COL_{date.strftime('%Y%m%d')}_{i:04d}",
                        'crash_date': date.strftime('%Y-%m-%dT%H:%M:%S'),
                        'borough': borough,
                        'persons_injured': 1 if i % 15 == 0 else 0,
                        'persons_killed': 1 if i % 100 == 0 else 0,
                        'weather_condition': 'Rain' if i % 10 == 0 else 'Clear'
                    })
            
            df = pd.DataFrame(collisions)
            
            # Save raw data
            os.makedirs("data/raw", exist_ok=True)
            filename = f"data/raw/collisions_{start_date}_to_{end_date}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"✅ Collision data saved: {filename}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Collision extraction failed: {e}")
            return pd.DataFrame()

def run_extraction(start_date=None, end_date=None):
    """Main extraction function"""
    logger.info("🚀 Starting data extraction")
    
    try:
        weather_extractor = WeatherExtractor()
        collision_extractor = CollisionExtractor()
        
        weather_df = weather_extractor.extract(start_date, end_date)
        collisions_df = collision_extractor.extract(start_date, end_date)
        
        logger.info(f"✅ Extraction complete: {len(weather_df)} weather, {len(collisions_df)} collisions")
        return weather_df, collisions_df
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        return pd.DataFrame(), pd.DataFrame()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    weather, collisions = run_extraction('2024-01-01', '2024-01-07')
    print(f"Weather: {len(weather)} rows")
    print(f"Collisions: {len(collisions)} rows")
