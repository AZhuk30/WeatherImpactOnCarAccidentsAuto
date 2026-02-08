import pandas as pd
import numpy as np
import os

print("Creating sample data files...")

# Create directories
os.makedirs("data/processed", exist_ok=True)

# Generate 30 days of sample data
dates = pd.date_range('2024-12-01', periods=30, freq='D')

# Sample collisions data
np.random.seed(42)
collisions_data = pd.DataFrame({
    'date': dates,
    'collisions': np.random.poisson(120, 30),
    'borough': np.random.choice(['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'], 30),
    'weather_condition': np.random.choice(['Clear', 'Rain', 'Snow', 'Fog'], 30),
    'severity': np.random.choice(['Minor', 'Moderate', 'Severe'], 30),
    'latitude': np.random.uniform(40.5, 40.9, 30),
    'longitude': np.random.uniform(-74.3, -73.7, 30)
})

# Sample weather data
weather_data = pd.DataFrame({
    'date': dates,
    'temperature': np.random.normal(45, 10, 30),
    'precipitation': np.random.exponential(0.1, 30),
    'snow_depth': np.where(dates.month.isin([12, 1, 2]), np.random.exponential(1, 30), 0),
    'condition': np.random.choice(['Clear', 'Rain', 'Snow', 'Fog'], 30),
    'wind_speed': np.random.exponential(5, 30),
    'humidity': np.random.uniform(50, 90, 30)
})

# Save to CSV
collisions_data.to_csv("data/processed/collisions_master.csv", index=False)
weather_data.to_csv("data/processed/weather_master.csv", index=False)

print(f"Collisions data shape: {collisions_data.shape}")
print(f"Weather data shape: {weather_data.shape}")
print("Sample data created successfully!")
