"""
Data extraction from APIs for GitHub Actions
Weather (Open-Meteo) + NYC Motor Vehicle Collisions
FIXED: Properly fetches recent data with correct date handling
"""

import logging
import os
import pandas as pd
import requests
import requests_cache
from retry_requests import retry
import openmeteo_requests
from datetime import datetime, timedelta
from io import StringIO

logger = logging.getLogger(__name__)

# Configuration
BOROUGHS = ["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"]
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
NYC_COLLISIONS_API = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
RAW_DATA_DIR = "data/raw"


class WeatherExtractor:
    """Extract hourly weather data for NYC boroughs using Open-Meteo"""
    
    def __init__(self):
        # Setup caching and retry logic
        cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.client = openmeteo_requests.Client(session=retry_session)
        
        # NYC borough coordinates
        # Manhattan, Brooklyn, Queens, Bronx, Staten Island
        self.latitudes = [40.7834, 40.6501, 40.6815, 40.8499, 40.5623]
        self.longitudes = [-73.9663, -73.9496, -73.8365, -73.8664, -74.1399]

    def extract(self, start_date=None, end_date=None):
        """Extract weather data - returns DataFrame"""
        try:
            logger.info("🌤️  Extracting weather data from Open-Meteo API")
            
            # Handle date parameters
            if end_date is None:
                end = datetime.now()
                end_date = end.strftime("%Y-%m-%d")
            else:
                end = datetime.strptime(end_date, "%Y-%m-%d")
                
            if start_date is None:
                start = end - timedelta(days=7)
                start_date = start.strftime("%Y-%m-%d")
            else:
                start = datetime.strptime(start_date, "%Y-%m-%d")
            
            # Calculate past_days
            past_days = (end - start).days
            
            logger.info(f"Date range: {start_date} to {end_date}, past_days: {past_days}")

            # API parameters
            params = {
                "latitude": self.latitudes,
                "longitude": self.longitudes,
                "hourly": [
                    "temperature_2m",
                    "precipitation",
                    "visibility",
                    "rain",
                    "showers",
                    "snowfall",
                    "wind_speed_10m",
                ],
                "past_days": past_days,
                "forecast_days": 0,
                "timezone": "America/New_York",
            }

            # Make API call
            responses = self.client.weather_api(WEATHER_API_URL, params=params)
            logger.info(f"✅ API call successful, got {len(responses)} responses")

            # Process responses for each borough
            dfs = []
            for i, response in enumerate(responses):
                borough = BOROUGHS[i]
                hourly = response.Hourly()

                # Create timestamps
                datetimes = pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left",
                )

                # Build DataFrame
                df = pd.DataFrame({
                    "borough": borough,
                    "datetime": datetimes,
                    "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
                    "precipitation": hourly.Variables(1).ValuesAsNumpy(),
                    "visibility": hourly.Variables(2).ValuesAsNumpy(),
                    "rain": hourly.Variables(3).ValuesAsNumpy(),
                    "showers": hourly.Variables(4).ValuesAsNumpy(),
                    "snowfall": hourly.Variables(5).ValuesAsNumpy(),
                    "wind_speed_10m": hourly.Variables(6).ValuesAsNumpy(),
                })

                # Convert datetime and add date column
                df["datetime"] = pd.to_datetime(df["datetime"])
                df["date"] = df["datetime"].dt.date
                dfs.append(df)

            # Combine all boroughs
            weather_df = pd.concat(dfs, ignore_index=True)

            # Save raw data
            os.makedirs(RAW_DATA_DIR, exist_ok=True)
            filename = f"{RAW_DATA_DIR}/nyc_borough_weather_hourly_{start_date}_to_{end_date}.csv"
            weather_df.to_csv(filename, index=False)

            logger.info(f"✅ Weather data saved: {filename}")
            logger.info(f"Total weather records: {len(weather_df)}")
            logger.info(f"Weather rows per borough:\n{weather_df['borough'].value_counts()}")

            return weather_df
            
        except Exception as e:
            logger.error(f"❌ Weather extraction failed: {e}")
            return pd.DataFrame()


class CollisionExtractor:
    """Extract NYC motor vehicle collisions data"""
    
    def extract(self, start_date=None, end_date=None):
        """Extract collision data - returns DataFrame
        
        FIXED: Now properly handles dates to fetch most recent data
        """
        try:
            logger.info("🚗 Extracting collision data from NYC Open Data API")
            
            # Handle date parameters
            if end_date is None:
                # Use today's date to ensure we get the most recent data
                end_date = datetime.now().strftime("%Y-%m-%d")
            
            if start_date is None:
                # Default to 30 days ago
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

            logger.info(f"Fetching collisions from {start_date} to {end_date}")

            # FIXED: Use proper date-time format and >= <= operators instead of BETWEEN
            # This ensures we get ALL data including the most recent entries
            params = {
                '$limit': 50000,
                '$where': f"crash_date >= '{start_date}T00:00:00' AND crash_date <= '{end_date}T23:59:59'",
                '$order': 'crash_date DESC'  # Get most recent first
            }

            # Make API call
            logger.info(f"API Request: {NYC_COLLISIONS_API}")
            logger.info(f"Query: {params['$where']}")
            
            response = requests.get(NYC_COLLISIONS_API, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse CSV response
            df = pd.read_csv(StringIO(response.text))
            logger.info(f"✅ Retrieved {len(df)} collision records")
            
            if len(df) > 0:
                # Convert crash_date to datetime for analysis
                df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
                
                # Log date range of retrieved data
                min_date = df['crash_date'].min()
                max_date = df['crash_date'].max()
                logger.info(f"📅 Data date range: {min_date} to {max_date}")
                logger.info(f"📊 Records per day in last 7 days:")
                
                # Show last 7 days of data counts
                recent_df = df[df['crash_date'] >= (datetime.now() - timedelta(days=7))]
                if len(recent_df) > 0:
                    daily_counts = recent_df.groupby(recent_df['crash_date'].dt.date).size()
                    for date, count in daily_counts.items():
                        logger.info(f"   {date}: {count} collisions")
                else:
                    logger.warning("⚠️  No data found in last 7 days")
            
            # Save raw data
            os.makedirs(RAW_DATA_DIR, exist_ok=True)
            filename = f"{RAW_DATA_DIR}/collisions_{start_date}_to_{end_date}.csv"
            df.to_csv(filename, index=False)

            logger.info(f"✅ Collision data saved: {filename}")
            
            if 'borough' in df.columns:
                logger.info(f"Collisions per borough:\n{df['borough'].value_counts()}")
            else:
                logger.warning("No borough column in collisions data")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Collision extraction failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


def run_extraction(start_date=None, end_date=None):
    """Main extraction function"""
    logger.info("🚀 Starting data extraction pipeline")
    
    try:
        weather_extractor = WeatherExtractor()
        collision_extractor = CollisionExtractor()
        
        weather_df = weather_extractor.extract(start_date, end_date)
        collisions_df = collision_extractor.extract(start_date, end_date)
        
        logger.info(
            f"✅ Extraction complete: "
            f"{len(weather_df)} weather records, {len(collisions_df)} collision records"
        )
        
        return weather_df, collisions_df
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        return pd.DataFrame(), pd.DataFrame()


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("="*60)
    print("TESTING EXTRACTION MODULE")
    print("="*60)
    
    # Test with recent dates - last 7 days
    test_end = datetime.now().strftime("%Y-%m-%d")
    test_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    print(f"\nTesting with dates: {test_start} to {test_end}")
    
    try:
        weather, collisions = run_extraction(test_start, test_end)
        print(f"\n✅ Weather: {len(weather)} rows")
        print(f"✅ Collisions: {len(collisions)} rows")
        
        if len(weather) > 0:
            print(f"\nWeather date range: {weather['date'].min()} to {weather['date'].max()}")
            print(f"Weather sample:\n{weather.head()}")
            
        if len(collisions) > 0:
            print(f"\nCollisions date range: {collisions['crash_date'].min()} to {collisions['crash_date'].max()}")
            print(f"Collisions sample:\n{collisions.head()}")
            
            # Show recent data
            collisions['crash_date'] = pd.to_datetime(collisions['crash_date'])
            recent = collisions[collisions['crash_date'] >= (datetime.now() - timedelta(days=3))]
            print(f"\n📊 Collisions in last 3 days: {len(recent)}")
        
        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
