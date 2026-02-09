"""
Data loading module - saves transformed data to CSV files
CSV-only mode for GitHub Actions / Streamlit Cloud
"""

import logging
import os
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


def run_loading(weather_df: pd.DataFrame, collisions_df: pd.DataFrame) -> bool:
    """
    Save transformed data to master CSV files
    
    Args:
        weather_df: Cleaned weather DataFrame
        collisions_df: Cleaned collisions DataFrame
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    logger.info("💾 Saving data to master CSV files")
    
    try:
        # Create output directory
        output_dir = "data/processed"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save weather data
        weather_file = f"{output_dir}/weather_master.csv"
        weather_df.to_csv(weather_file, index=False)
        logger.info(f"✅ Weather data saved: {weather_file} ({len(weather_df)} rows)")
        
        # Save collisions data
        collisions_file = f"{output_dir}/collisions_master.csv"
        collisions_df.to_csv(collisions_file, index=False)
        logger.info(f"✅ Collisions data saved: {collisions_file} ({len(collisions_df)} rows)")
        
        # Log file sizes
        weather_size = os.path.getsize(weather_file)
        collisions_size = os.path.getsize(collisions_file)
        
        logger.info(f"📊 File sizes:")
        logger.info(f"   Weather: {weather_size:,} bytes")
        logger.info(f"   Collisions: {collisions_size:,} bytes")
        
        # Create metadata file
        metadata = {
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'weather_rows': len(weather_df),
            'collisions_rows': len(collisions_df),
            'weather_file': weather_file,
            'collisions_file': collisions_file
        }
        
        metadata_file = f"{output_dir}/metadata.txt"
        with open(metadata_file, 'w') as f:
            for key, value in metadata.items():
                f.write(f"{key}: {value}\n")
        
        logger.info(f"✅ Metadata saved: {metadata_file}")
        logger.info("✅ All data successfully saved to CSV files")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to save data: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Test the loading module
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data
    weather_test = pd.DataFrame({
        'date': ['2026-01-01', '2026-01-02'],
        'borough': ['MANHATTAN', 'BROOKLYN'],
        'temperature': [10, 12]
    })
    
    collisions_test = pd.DataFrame({
        'crash_date': ['2026-01-01', '2026-01-02'],
        'borough': ['MANHATTAN', 'BROOKLYN'],
        'persons_injured': [1, 0]
    })
    
    success = run_loading(weather_test, collisions_test)
    print(f"\nTest {'PASSED' if success else 'FAILED'}")
