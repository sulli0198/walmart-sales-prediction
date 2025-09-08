import os
import requests
import psycopg2
import pandas as pd
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_environment_variables():
    """Load environment variables from .env file"""
    load_dotenv()
    
    # Alpha Vantage API
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    # Database credentials
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'walmart_sales_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY not found in environment variables")
    if not db_config['password']:
        raise ValueError("DB_PASSWORD not found in environment variables")
    
    logger.info("Environment variables loaded successfully")
    return api_key, db_config

def fetch_walmart_stock_data(api_key):
    """Fetch Walmart stock data from Alpha Vantage API"""
    
    # Alpha Vantage URL for daily adjusted stock data
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'symbol': 'WMT',  # Walmart stock symbol
        'apikey': api_key,
        'outputsize': 'full'  # Gets up to 20 years of data (change from 'compact')
    }
    
    logger.info("Fetching Walmart stock data from Alpha Vantage...")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Check for API error messages
        if "Error Message" in data:
            raise ValueError(f"Alpha Vantage API Error: {data['Error Message']}")
        if "Note" in data:
            raise ValueError(f"Alpha Vantage API Note: {data['Note']} (You might have hit rate limit)")
        
        # Extract time series data
        if "Time Series (Daily)" not in data:
            raise ValueError("No time series data found in API response")
        
        time_series = data["Time Series (Daily)"]
        logger.info(f"Successfully fetched data for {len(time_series)} trading days")
        
        return time_series
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from Alpha Vantage: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing Alpha Vantage response: {e}")
        raise

def fetch_historical_weather_data(start_date, end_date, city="Bentonville"):
    """Fetch historical weather data from Open-Meteo API"""
    
    # City coordinates (Walmart HQ: Bentonville, Arkansas)
    city_coords = {
        "Bentonville": {"lat": 36.37, "lon": -94.21},
        "New York": {"lat": 40.71, "lon": -74.01}
    }
    
    if city not in city_coords:
        city = "Bentonville"  # Default fallback
    
    coords = city_coords[city]
    
    # Open-Meteo Historical Weather API
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': coords['lat'],
        'longitude': coords['lon'],
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min', 
            'temperature_2m_mean',
            'relative_humidity_2m_max',
            'surface_pressure',
            'windspeed_10m_max',
            'precipitation_sum'
        ],
        'timezone': 'America/Chicago'  # Arkansas timezone
    }
    
    logger.info(f"Fetching weather data for {city} from {start_date} to {end_date}...")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'daily' not in data:
            raise ValueError("No daily weather data found in API response")
        
        daily_data = data['daily']
        dates = daily_data['time']
        
        logger.info(f"Successfully fetched weather data for {len(dates)} days")
        
        return daily_data, city
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather data: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing weather response: {e}")
        raise

def process_stock_data(time_series_data):
    """Process and clean the stock data"""
    
    logger.info("Processing stock data...")
    processed_data = []
    
    for date_str, daily_data in time_series_data.items():
        try:
            # Convert date string to date object
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Only get last year of data to match weather
            one_year_ago = date.today() - timedelta(days=365)
            if date_obj < one_year_ago:
                continue
            
            # Extract and convert stock data
            record = {
                'date': date_obj,
                'open_price': float(daily_data['1. open']),
                'high_price': float(daily_data['2. high']),
                'low_price': float(daily_data['3. low']),
                'close_price': float(daily_data['4. close']),
                'adjusted_close': float(daily_data['5. adjusted close']),
                'volume': int(daily_data['6. volume']),
                'dividend_amount': float(daily_data['7. dividend amount']),
                'split_coefficient': float(daily_data['8. split coefficient'])
            }
            
            processed_data.append(record)
            
        except (ValueError, KeyError) as e:
            logger.warning(f"Error processing data for date {date_str}: {e}")
            continue
    
    logger.info(f"Successfully processed {len(processed_data)} stock records")
    return processed_data

def process_weather_data(daily_weather_data, city):
    """Process and clean the weather data"""
    
    logger.info("Processing weather data...")
    processed_data = []
    
    dates = daily_weather_data['time']
    
    for i, date_str in enumerate(dates):
        try:
            # Convert date string to date object
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Extract weather data (handle potential None values)
            temp_max = daily_weather_data['temperature_2m_max'][i]
            temp_min = daily_weather_data['temperature_2m_min'][i]
            temp_mean = daily_weather_data['temperature_2m_mean'][i]
            humidity = daily_weather_data['relative_humidity_2m_max'][i]
            pressure = daily_weather_data['surface_pressure'][i]
            wind_speed = daily_weather_data['windspeed_10m_max'][i]
            precipitation = daily_weather_data['precipitation_sum'][i]
            
            record = {
                'date': date_obj,
                'city': city,
                'temperature_avg': temp_mean,
                'temperature_min': temp_min,
                'temperature_max': temp_max,
                'humidity': int(humidity) if humidity is not None else None,
                'pressure': pressure,
                'wind_speed': wind_speed,
                'weather_condition': 'Clear' if precipitation == 0 else 'Precipitation',
                'weather_description': f'Precipitation: {precipitation}mm' if precipitation > 0 else 'Clear day',
                'visibility': 10.0,  # Default visibility
                'uv_index': 5.0  # Default UV index
            }
            
            processed_data.append(record)
            
        except (ValueError, KeyError, IndexError) as e:
            logger.warning(f"Error processing weather data for date {date_str}: {e}")
            continue
    
    logger.info(f"Successfully processed {len(processed_data)} weather records")
    return processed_data

def connect_to_database(db_config):
    """Connect to PostgreSQL database"""
    
    try:
        logger.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**db_config)
        logger.info("Database connection successful")
        return conn
        
    except psycopg2.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def insert_stock_data(conn, stock_data):
    """Insert stock data into walmart_raw_data table"""
    
    try:
        cursor = conn.cursor()
        
        # SQL insert query
        insert_query = """
        INSERT INTO walmart_raw_data 
        (date, open_price, high_price, low_price, close_price, adjusted_close, 
         volume, dividend_amount, split_coefficient)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            adjusted_close = EXCLUDED.adjusted_close,
            volume = EXCLUDED.volume,
            dividend_amount = EXCLUDED.dividend_amount,
            split_coefficient = EXCLUDED.split_coefficient
        """
        
        logger.info("Inserting stock data into database...")
        
        # Insert data
        inserted_count = 0
        for record in stock_data:
            cursor.execute(insert_query, (
                record['date'],
                record['open_price'],
                record['high_price'],
                record['low_price'],
                record['close_price'],
                record['adjusted_close'],
                record['volume'],
                record['dividend_amount'],
                record['split_coefficient']
            ))
            inserted_count += 1
        
        # Commit the transaction
        conn.commit()
        cursor.close()
        
        logger.info(f"Successfully inserted/updated {inserted_count} stock records")
        return inserted_count
        
    except psycopg2.Error as e:
        logger.error(f"Error inserting stock data into database: {e}")
        conn.rollback()
        raise

def insert_weather_data(conn, weather_data):
    """Insert weather data into weather_raw_data table"""
    
    try:
        cursor = conn.cursor()
        
        # SQL insert query
        insert_query = """
        INSERT INTO weather_raw_data 
        (date, city, temperature_avg, temperature_min, temperature_max, 
         humidity, pressure, wind_speed, weather_condition, weather_description,
         visibility, uv_index)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, city) DO UPDATE SET
            temperature_avg = EXCLUDED.temperature_avg,
            temperature_min = EXCLUDED.temperature_min,
            temperature_max = EXCLUDED.temperature_max,
            humidity = EXCLUDED.humidity,
            pressure = EXCLUDED.pressure,
            wind_speed = EXCLUDED.wind_speed,
            weather_condition = EXCLUDED.weather_condition,
            weather_description = EXCLUDED.weather_description,
            visibility = EXCLUDED.visibility,
            uv_index = EXCLUDED.uv_index
        """
        
        logger.info("Inserting weather data into database...")
        
        # Insert data
        inserted_count = 0
        for record in weather_data:
            cursor.execute(insert_query, (
                record['date'],
                record['city'],
                record['temperature_avg'],
                record['temperature_min'],
                record['temperature_max'],
                record['humidity'],
                record['pressure'],
                record['wind_speed'],
                record['weather_condition'],
                record['weather_description'],
                record['visibility'],
                record['uv_index']
            ))
            inserted_count += 1
        
        # Commit the transaction
        conn.commit()
        cursor.close()
        
        logger.info(f"Successfully inserted/updated {inserted_count} weather records")
        return inserted_count
        
    except psycopg2.Error as e:
        logger.error(f"Error inserting weather data into database: {e}")
        conn.rollback()
        raise

def verify_data_insertion(conn):
    """Verify that both stock and weather data were inserted correctly"""
    
    try:
        cursor = conn.cursor()
        
        # Count records in both tables
        cursor.execute("SELECT COUNT(*) FROM walmart_raw_data")
        stock_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM weather_raw_data")
        weather_count = cursor.fetchone()[0]
        
        # Get date ranges
        cursor.execute("""
            SELECT MIN(date) as earliest_date, MAX(date) as latest_date 
            FROM walmart_raw_data
        """)
        stock_dates = cursor.fetchone()
        
        cursor.execute("""
            SELECT MIN(date) as earliest_date, MAX(date) as latest_date 
            FROM weather_raw_data
        """)
        weather_dates = cursor.fetchone()
        
        cursor.close()
        
        logger.info(f"Data verification complete:")
        logger.info(f"  Stock records: {stock_count} (from {stock_dates[0]} to {stock_dates[1]})")
        logger.info(f"  Weather records: {weather_count} (from {weather_dates[0]} to {weather_dates[1]})")
        
        return stock_count > 0 and weather_count > 0
        
    except psycopg2.Error as e:
        logger.error(f"Error verifying data insertion: {e}")
        return False

def main():
    """Main function to orchestrate the data ingestion process"""
    
    try:
        logger.info("Starting data ingestion process (Stock + Weather)...")
        
        # Load environment variables
        api_key, db_config = load_environment_variables()
        
        # Fetch stock data from Alpha Vantage
        time_series_data = fetch_walmart_stock_data(api_key)
        
        # Process stock data
        processed_stock_data = process_stock_data(time_series_data)
        
        if not processed_stock_data:
            logger.error("No valid stock data to insert")
            return False
        
        # Get date range from stock data for weather fetching
        stock_dates = [record['date'] for record in processed_stock_data]
        start_date = min(stock_dates)
        end_date = max(stock_dates)
        
        # Fetch weather data for the same date range
        weather_daily_data, city = fetch_historical_weather_data(start_date, end_date)
        
        # Process weather data
        processed_weather_data = process_weather_data(weather_daily_data, city)
        
        # Connect to database
        conn = connect_to_database(db_config)
        
        try:
            # Insert stock data
            stock_inserted = insert_stock_data(conn, processed_stock_data)
            
            # Insert weather data
            weather_inserted = insert_weather_data(conn, processed_weather_data)
            
            # Verify insertion
            verification_success = verify_data_insertion(conn)
            
            if verification_success:
                logger.info("✅ Data ingestion completed successfully!")
                return True
            else:
                logger.error("❌ Data verification failed")
                return False
                
        finally:
            conn.close()
            logger.info("Database connection closed")
            
    except Exception as e:
        logger.error(f"❌ Data ingestion failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 SUCCESS: Stock and weather data loaded into your database!")
        print("💡 Data summary:")
        print("   • Walmart stock data (last year)")
        print("   • Historical weather data from Open-Meteo")
        print("   • Data aligned by date for analysis")
        print("\n📊 Next steps:")
        print("   1. Check your data in pgAdmin")
        print("   2. Move on to SQL transformations (Week 2)")
    else:
        print("\n❌ FAILED: Check the logs above for details")
        print("💡 Common fixes:")
        print("   1. Verify your .env file has correct API key and DB password")
        print("   2. Make sure PostgreSQL is running")
        print("   3. Check your internet connection")