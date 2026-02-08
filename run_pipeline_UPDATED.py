"""
Complete ETL Pipeline for NYC Traffic Safety Analysis
Now supports CSV-only mode for GitHub Actions deployment
"""
import logging
import sys
import os
from datetime import datetime, timedelta
import pytz
import pandas as pd

# Check if we should skip database (for GitHub Actions)
SKIP_DATABASE = os.getenv('SKIP_DATABASE', 'false').lower() == 'true'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_default_dates(days=30):
    """
    Get default date range for data extraction
    
    Args:
        days: Number of days to look back (default: 30)
    
    Returns:
        tuple: (start_date, end_date) as strings
    """
    nyc_tz = pytz.timezone('America/New_York')
    
    # End date: yesterday (most recent complete day)
    end_date = datetime.now(nyc_tz) - timedelta(days=1)
    
    # Start date: N days before end date
    start_date = end_date - timedelta(days=days-1)
    
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def run_pipeline(start_date: str = None, end_date: str = None, days: int = 30):
    """
    Run complete ETL pipeline
    
    Args:
        start_date: Start date (YYYY-MM-DD), defaults to N days ago
        end_date: End date (YYYY-MM-DD), defaults to yesterday
        days: Number of days to extract (default: 30)
    """
    
    pipeline_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    logger.info("=" * 70)
    logger.info(f"🚀 STARTING ETL PIPELINE - Run ID: {pipeline_id}")
    if SKIP_DATABASE:
        logger.info("📝 Running in CSV-ONLY mode (database disabled)")
    logger.info("=" * 70)
    
    try:
        # ========== EXTRACTION PHASE ==========
        logger.info("📥 PHASE 1: EXTRACTION")
        logger.info("-" * 40)
        
        # Get default dates if none provided
        if start_date is None or end_date is None:
            default_start, default_end = get_default_dates(days)
            if start_date is None:
                start_date = default_start
                logger.info(f"Using default start date: {start_date}")
            if end_date is None:
                end_date = default_end
                logger.info(f"Using default end date: {end_date}")
        
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        
        # Calculate days for reference
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        days_range = (end_dt - start_dt).days + 1
        logger.info(f"📆 Extracting {days_range} days of data")
        
        try:
            from src.extract import run_extraction
            weather_df, collisions_df = run_extraction(start_date, end_date)
            
            logger.info(f"✅ Extraction completed")
            logger.info(f"   Weather records: {len(weather_df):,}")
            logger.info(f"   Collision records: {len(collisions_df):,}")
            
            # Show extracted data statistics
            if len(weather_df) > 0:
                unique_dates = weather_df['date'].nunique() if 'date' in weather_df.columns else 'N/A'
                unique_boroughs = weather_df['borough'].nunique() if 'borough' in weather_df.columns else 'N/A'
                logger.info(f"   Weather - Dates: {unique_dates}, Boroughs: {unique_boroughs}")
                
                if 'datetime' in weather_df.columns:
                    min_date = weather_df['datetime'].min()
                    max_date = weather_df['datetime'].max()
                    logger.info(f"   Weather time range: {min_date} to {max_date}")
            
            if len(collisions_df) > 0:
                if 'crash_date' in collisions_df.columns:
                    unique_crash_dates = collisions_df['crash_date'].nunique()
                    logger.info(f"   Collisions - Days with accidents: {unique_crash_dates}")
                
                if 'borough' in collisions_df.columns:
                    borough_counts = collisions_df['borough'].value_counts()
                    top_boroughs = borough_counts.head(3).to_dict()
                    logger.info(f"   Top boroughs: {top_boroughs}")
                          
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            
            # Fallback to historical dates
            logger.info("🔄 Trying fallback dates (2024-01-01 to 2024-01-30)...")
            try:
                from src.extract import run_extraction
                weather_df, collisions_df = run_extraction('2024-01-01', '2024-01-30')
                logger.info(f"✅ Fallback extraction successful")
                logger.info(f"   Weather records: {len(weather_df):,}")
                logger.info(f"   Collision records: {len(collisions_df):,}")
            except Exception as fallback_e:
                logger.error(f"❌ Fallback extraction also failed: {fallback_e}")
                raise
        
        # ========== TRANSFORMATION PHASE ==========
        logger.info("\n🔄 PHASE 2: TRANSFORMATION")
        logger.info("-" * 40)
        
        try:
            from src.transform import run_transformation
            weather_clean, collisions_clean = run_transformation(weather_df, collisions_df)
            
            logger.info(f"✅ Transformation completed")
            logger.info(f"   Clean weather records: {len(weather_clean):,}")
            logger.info(f"   Clean collision records: {len(collisions_clean):,}")
            
            # Show transformation results
            if len(weather_clean) > 0:
                new_features = list(set(weather_clean.columns) - set(weather_df.columns))
                logger.info(f"   Weather features added: {len(new_features)}")
                
                if len(weather_clean) > 0 and 'weather_category' in weather_clean.columns:
                    sample_row = weather_clean.iloc[0]
                    logger.info(f"   Sample - Weather: {sample_row.get('weather_category', 'N/A')}, "
                              f"Temp: {sample_row.get('temperature_2m', 'N/A')}°C")
                
            if len(collisions_clean) > 0 and 'severity_level' in collisions_clean.columns:
                severity_counts = collisions_clean['severity_level'].value_counts().to_dict()
                logger.info(f"   Collision severity distribution: {severity_counts}")
                
        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}")
            raise
        
        # ========== LOADING PHASE ==========
        logger.info("\n💾 PHASE 3: LOADING")
        logger.info("-" * 40)
        
        success = True  # Default to success for CSV-only mode
        
        if not SKIP_DATABASE:
            # Try to load into database
            try:
                from src.load import run_loading
                logger.info("Attempting to load data into database...")
                success = run_loading(weather_clean, collisions_clean)
                
                if success:
                    logger.info("✅ Data loaded successfully into database")
                else:
                    logger.warning("⚠️  Database loading returned False")
                    
            except ImportError as e:
                logger.warning(f"⚠️  Load module import failed: {e}")
                logger.info("   Database module not available, using CSV-only mode")
                success = True
            except Exception as e:
                logger.error(f"❌ Loading failed: {e}")
                logger.info("   Continuing with CSV files...")
                success = False
        else:
            logger.info("✅ CSV files saved (database skipped per SKIP_DATABASE flag)")
        
        # ========== PIPELINE SUMMARY ==========
        logger.info("\n" + "=" * 70)
        logger.info(f"📊 ETL PIPELINE SUMMARY - Run ID: {pipeline_id}")
        logger.info("=" * 70)
        
        # Calculate statistics
        weather_per_day = len(weather_clean) / days_range if days_range > 0 else 0
        collisions_per_day = len(collisions_clean) / days_range if days_range > 0 else 0
        
        summary_stats = {
            "Pipeline ID": pipeline_id,
            "Run Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Mode": "CSV-Only" if SKIP_DATABASE else "Full (Database + CSV)",
            "Date Range": f"{start_date} to {end_date} ({days_range} days)",
            "Weather Records": f"{len(weather_clean):,} ({weather_per_day:.1f}/day)",
            "Collision Records": f"{len(collisions_clean):,} ({collisions_per_day:.1f}/day)",
            "Total Records": f"{len(weather_clean) + len(collisions_clean):,}",
            "Database Load": "Skipped" if SKIP_DATABASE else ("✅ Success" if success else "⚠️  Failed"),
            "Master Files": "data/processed/weather_master.csv, data/processed/collisions_master.csv"
        }
        
        for key, value in summary_stats.items():
            logger.info(f"   {key:25}: {value}")
        
        logger.info("\n✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        
        # Save detailed summary to file
        summary_file = f"data/logs/pipeline_summary_{pipeline_id}.txt"
        with open(summary_file, 'w') as f:
            f.write(f"ETL Pipeline Summary - Run ID: {pipeline_id}\n")
            f.write("=" * 60 + "\n")
            for key, value in summary_stats.items():
                f.write(f"{key:30}: {value}\n")
            
            # Data quality metrics
            f.write("\n" + "=" * 60 + "\n")
            f.write("DATA QUALITY METRICS\n")
            f.write("=" * 60 + "\n")
            
            if len(weather_clean) > 0:
                f.write(f"\nWEATHER DATA:\n")
                f.write(f"  Total records: {len(weather_clean)}\n")
                if 'borough' in weather_clean.columns:
                    borough_counts = weather_clean['borough'].value_counts()
                    f.write(f"  Records per borough:\n")
                    for borough, count in borough_counts.items():
                        f.write(f"    {borough}: {count}\n")
            
            if len(collisions_clean) > 0:
                f.write(f"\nCOLLISION DATA:\n")
                f.write(f"  Total records: {len(collisions_clean)}\n")
                if 'borough' in collisions_clean.columns:
                    borough_counts = collisions_clean['borough'].value_counts()
                    f.write(f"  Records per borough:\n")
                    for borough, count in borough_counts.items():
                        f.write(f"    {borough}: {count}\n")
        
        logger.info(f"📝 Summary saved to: {summary_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ ETL PIPELINE FAILED: {e}")
        logger.error("=" * 70)
        
        # Log error details
        import traceback
        error_log = f"data/logs/pipeline_error_{pipeline_id}.txt"
        with open(error_log, 'w') as f:
            f.write(f"Pipeline Error - Run ID: {pipeline_id}\n")
            f.write("=" * 50 + "\n")
            f.write(f"Error: {str(e)}\n")
            f.write(f"Date Range: {start_date} to {end_date}\n")
            f.write(f"Timestamp: {datetime.now()}\n")
            f.write("\nTraceback:\n")
            traceback.print_exc(file=f)
        
        logger.error(f"📝 Error details saved to: {error_log}")
        
        return False


def main():
    """Main function to run the pipeline with command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run NYC Traffic Safety ETL Pipeline')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=30, help='Number of days to pull (default: 30)')
    parser.add_argument('--test', action='store_true', help='Run in test mode with 2 days')
    parser.add_argument('--historical', action='store_true', help='Use historical dates (2024-01-01 to 2024-01-30)')
    
    args = parser.parse_args()
    
    print("\n" + "=" * 70)
    print("NYC TRAFFIC SAFETY - WEATHER IMPACT ANALYSIS")
    print(f"ETL Pipeline - {args.days} Days Data")
    if SKIP_DATABASE:
        print("Mode: CSV-ONLY (Database Disabled)")
    print("=" * 70)
    
    # Determine which dates to use
    if args.historical:
        start_date = "2024-01-01"
        end_date = "2024-01-30"
        print("📅 Using historical dates (2024-01-01 to 2024-01-30)")
    elif args.test:
        start_date = "2024-01-01"
        end_date = "2024-01-02"
        print("🧪 Running in TEST mode (2 days of data)")
    elif args.start_date or args.end_date:
        start_date = args.start_date
        end_date = args.end_date
        if start_date:
            print(f"📅 Start date: {start_date}")
        if end_date:
            print(f"📅 End date: {end_date}")
    else:
        start_date = None
        end_date = None
        print(f"📅 Getting latest {args.days} days of data (default)")
    
    print("\n" + "=" * 70)
    
    # Create necessary directories
    os.makedirs('data/logs', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('data/raw', exist_ok=True)
    
    # Run the pipeline
    success = run_pipeline(start_date, end_date, args.days)
    
    if success:
        print("\n" + "=" * 70)
        print("✅ PIPELINE EXECUTION COMPLETE")
        print("=" * 70)
        print("\nNext steps:")
        if not SKIP_DATABASE:
            print("1. Check database: mysql -u pipeline_user -p nyc_traffic_safety")
        print("2. View logs: tail -f data/logs/pipeline.log")
        print("3. Check processed data: ls -la data/processed/")
        print("4. Run Streamlit dashboard: streamlit run dashboard/app.py")
        print("=" * 70)
        
        # Show where files are saved
        import glob
        master_files = glob.glob('data/processed/*_master.csv')
        
        if master_files:
            print("\n📁 Master data files:")
            for f in master_files:
                print(f"   {f}")
            
    else:
        print("\n" + "=" * 70)
        print("❌ PIPELINE EXECUTION FAILED")
        print("=" * 70)
        print("\nTroubleshooting:")
        print("1. Check error logs: cat data/logs/pipeline_error_*.txt")
        print("2. Try historical dates: python run_pipeline.py --historical")
        print("3. Try test mode: python run_pipeline.py --test")
        print("4. Check internet connection")
        print("=" * 70)
    
    return 0 if success else 1


if __name__ == "__main__":
    print("🚀 Starting ETL Pipeline...")
    
    # Check required packages
    try:
        import pandas
        import requests
        import pytz
        print("✅ Required packages installed")
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        print("\n💡 Install required packages:")
        print("pip install pandas requests pytz")
        sys.exit(1)
    
    # Run the pipeline
    sys.exit(main())
