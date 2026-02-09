"""
Enhanced data loading module - saves detailed data to CSV files
Keeps all the rich information from weather and collision APIs
"""

import logging
import os
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


def run_loading(weather_df: pd.DataFrame, collisions_df: pd.DataFrame) -> bool:
    """
    Save transformed data to master CSV files with FULL details
    
    Args:
        weather_df: Cleaned weather DataFrame (with all columns)
        collisions_df: Cleaned collisions DataFrame (with all columns)
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    logger.info("💾 Saving ENHANCED data to master CSV files")
    
    try:
        # Create output directory
        output_dir = "data/processed"
        os.makedirs(output_dir, exist_ok=True)
        
        # ========================================
        # ENHANCED WEATHER DATA
        # ========================================
        weather_cols = [
            'date',
            'borough',
            'datetime',
            'temperature_2m',
            'precipitation',
            'visibility',
            'rain',
            'showers', 
            'snowfall',
            'wind_speed_10m',
            'hour_nyc',
            'day_of_week',
            'is_weekend',
            'is_rush_hour',
            'is_night',
            'month',
            'season',
            'weather_category',
            'weather_severity'
        ]
        
        # Keep only columns that exist
        available_weather_cols = [col for col in weather_cols if col in weather_df.columns]
        weather_enhanced = weather_df[available_weather_cols].copy()
        
        weather_file = f"{output_dir}/weather_master.csv"
        weather_enhanced.to_csv(weather_file, index=False)
        logger.info(f"✅ Weather data saved: {weather_file}")
        logger.info(f"   Columns: {', '.join(available_weather_cols)}")
        logger.info(f"   Rows: {len(weather_enhanced):,}")
        
        # ========================================
        # ENHANCED COLLISION DATA
        # ========================================
        collision_cols = [
            'crash_date',
            'crash_time',
            'borough',
            'zip_code',
            'latitude',
            'longitude',
            'location',
            'on_street_name',
            'cross_street_name',
            'off_street_name',
            'number_of_persons_injured',
            'number_of_persons_killed',
            'number_of_pedestrians_injured',
            'number_of_pedestrians_killed',
            'number_of_cyclist_injured',
            'number_of_cyclist_killed',
            'number_of_motorist_injured',
            'number_of_motorist_killed',
            'contributing_factor_vehicle_1',
            'contributing_factor_vehicle_2',
            'vehicle_type_code1',
            'vehicle_type_code2',
            'collision_id',
            'date',
            'hour',
            'day_of_week',
            'is_weekend',
            'is_rush_hour',
            'is_night',
            'month',
            'season',
            'severity_level',
            'involved_pedestrian',
            'involved_cyclist',
            'involved_multiple_vehicles'
        ]
        
        # Keep only columns that exist
        available_collision_cols = [col for col in collision_cols if col in collisions_df.columns]
        collisions_enhanced = collisions_df[available_collision_cols].copy()
        
        collisions_file = f"{output_dir}/collisions_master.csv"
        collisions_enhanced.to_csv(collisions_file, index=False)
        logger.info(f"✅ Collisions data saved: {collisions_file}")
        logger.info(f"   Columns: {', '.join(available_collision_cols)}")
        logger.info(f"   Rows: {len(collisions_enhanced):,}")
        
        # ========================================
        # DAILY AGGREGATED SUMMARY (Optional)
        # ========================================
        # Create a daily summary for quick analysis
        if 'date' in weather_df.columns and 'borough' in weather_df.columns:
            daily_weather = weather_df.groupby(['date', 'borough']).agg({
                'temperature_2m': 'mean',
                'precipitation': 'sum',
                'rain': 'sum',
                'snowfall': 'sum',
                'wind_speed_10m': 'mean',
                'visibility': 'mean'
            }).reset_index()
            
            daily_weather.columns = [
                'date', 'borough', 
                'avg_temp', 'total_precip', 'total_rain', 
                'total_snow', 'avg_wind', 'avg_visibility'
            ]
            
            daily_weather_file = f"{output_dir}/weather_daily_summary.csv"
            daily_weather.to_csv(daily_weather_file, index=False)
            logger.info(f"✅ Daily weather summary saved: {daily_weather_file}")
        
        if 'date' in collisions_df.columns and 'borough' in collisions_df.columns:
            daily_collisions = collisions_df.groupby(['date', 'borough']).agg({
                'collision_id': 'count',
                'number_of_persons_injured': 'sum',
                'number_of_persons_killed': 'sum',
            }).reset_index()
            
            daily_collisions.columns = [
                'date', 'borough',
                'total_collisions', 'total_injured', 'total_killed'
            ]
            
            daily_collisions_file = f"{output_dir}/collisions_daily_summary.csv"
            daily_collisions.to_csv(daily_collisions_file, index=False)
            logger.info(f"✅ Daily collisions summary saved: {daily_collisions_file}")
        
        # ========================================
        # METADATA
        # ========================================
        metadata = {
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            'weather_rows': len(weather_enhanced),
            'collisions_rows': len(collisions_enhanced),
            'weather_columns': len(available_weather_cols),
            'collision_columns': len(available_collision_cols),
            'date_range_start': weather_df['date'].min() if 'date' in weather_df.columns else 'N/A',
            'date_range_end': weather_df['date'].max() if 'date' in weather_df.columns else 'N/A',
        }
        
        metadata_file = f"{output_dir}/data_info.txt"
        with open(metadata_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("NYC TRAFFIC SAFETY DATA - METADATA\n")
            f.write("=" * 60 + "\n\n")
            for key, value in metadata.items():
                f.write(f"{key}: {value}\n")
            
            f.write("\n" + "=" * 60 + "\n")
            f.write("WEATHER COLUMNS:\n")
            f.write("=" * 60 + "\n")
            for col in available_weather_cols:
                f.write(f"  - {col}\n")
            
            f.write("\n" + "=" * 60 + "\n")
            f.write("COLLISION COLUMNS:\n")
            f.write("=" * 60 + "\n")
            for col in available_collision_cols:
                f.write(f"  - {col}\n")
        
        logger.info(f"✅ Metadata saved: {metadata_file}")
        
        # Log file sizes
        weather_size = os.path.getsize(weather_file)
        collisions_size = os.path.getsize(collisions_file)
        
        logger.info(f"\n📊 FILE SIZES:")
        logger.info(f"   Weather: {weather_size:,} bytes ({weather_size/1024:.1f} KB)")
        logger.info(f"   Collisions: {collisions_size:,} bytes ({collisions_size/1024:.1f} KB)")
        logger.info("✅ All enhanced data successfully saved!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to save data: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Test the loading module
    logging.basicConfig(level=logging.INFO)
    
    print("Testing enhanced load module...")
    
    # Create sample data
    weather_test = pd.DataFrame({
        'date': ['2026-01-01', '2026-01-02'],
        'borough': ['MANHATTAN', 'BROOKLYN'],
        'datetime': ['2026-01-01 12:00:00', '2026-01-02 12:00:00'],
        'temperature_2m': [10, 12],
        'precipitation': [0.0, 2.5],
        'visibility': [10000, 5000],
        'rain': [0.0, 2.5],
        'snowfall': [0.0, 0.0],
        'wind_speed_10m': [5.0, 8.0],
        'weather_category': ['CLEAR', 'RAIN'],
        'weather_severity': ['LIGHT', 'MODERATE']
    })
    
    collisions_test = pd.DataFrame({
        'crash_date': ['2026-01-01', '2026-01-02'],
        'date': ['2026-01-01', '2026-01-02'],
        'borough': ['MANHATTAN', 'BROOKLYN'],
        'collision_id': ['123', '456'],
        'number_of_persons_injured': [1, 0],
        'number_of_persons_killed': [0, 0],
        'contributing_factor_vehicle_1': ['Driver Inattention', 'Speed'],
        'vehicle_type_code1': ['Sedan', 'SUV']
    })
    
    success = run_loading(weather_test, collisions_test)
    print(f"\n{'='*60}")
    print(f"Test {'✅ PASSED' if success else '❌ FAILED'}")
    print(f"{'='*60}")
