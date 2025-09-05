import os
import requests
import psycopg2
import pandas as pd
from datetime import datetime
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
        'outputsize': 'compact'  # Gets last 100 trading days
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

def process_stock_data(time_series_data):
    """Process and clean the stock data"""
    
    logger.info("Processing stock data...")
    processed_data = []
    
    for date_str, daily_data in time_series_data.items():
        try:
            # Convert date string to date object
            date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            
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
    
    logger.info(f"Successfully processed {len(processed_data)} records")
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
        
        logger.info(f"Successfully inserted/updated {inserted_count} records")
        return inserted_count
        
    except psycopg2.Error as e:
        logger.error(f"Error inserting data into database: {e}")
        conn.rollback()
        raise

def verify_data_insertion(conn):
    """Verify that data was inserted correctly"""
    
    try:
        cursor = conn.cursor()
        
        # Count total records
        cursor.execute("SELECT COUNT(*) FROM walmart_raw_data")
        total_count = cursor.fetchone()[0]
        
        # Get date range
        cursor.execute("""
            SELECT MIN(date) as earliest_date, MAX(date) as latest_date 
            FROM walmart_raw_data
        """)
        date_range = cursor.fetchone()
        
        # Get sample record
        cursor.execute("""
            SELECT date, close_price, volume 
            FROM walmart_raw_data 
            ORDER BY date DESC 
            LIMIT 1
        """)
        sample_record = cursor.fetchone()
        
        cursor.close()
        
        logger.info(f"Data verification complete:")
        logger.info(f"  Total records: {total_count}")
        logger.info(f"  Date range: {date_range[0]} to {date_range[1]}")
        logger.info(f"  Latest record: Date={sample_record[0]}, Close=${sample_record[1]}, Volume={sample_record[2]:,}")
        
        return total_count > 0
        
    except psycopg2.Error as e:
        logger.error(f"Error verifying data insertion: {e}")
        return False

def main():
    """Main function to orchestrate the data ingestion process"""
    
    try:
        logger.info("Starting Walmart stock data ingestion process...")
        
        # Load environment variables
        api_key, db_config = load_environment_variables()
        
        # Fetch stock data from Alpha Vantage
        time_series_data = fetch_walmart_stock_data(api_key)
        
        # Process the data
        processed_data = process_stock_data(time_series_data)
        
        if not processed_data:
            logger.error("No valid data to insert")
            return False
        
        # Connect to database
        conn = connect_to_database(db_config)
        
        try:
            # Insert data into database
            inserted_count = insert_stock_data(conn, processed_data)
            
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
        print("\n🎉 SUCCESS: Walmart stock data has been loaded into your database!")
        print("💡 Next steps:")
        print("   1. Check your data in pgAdmin")
        print("   2. Add weather API when ready")
        print("   3. Move on to SQL transformations")
    else:
        print("\n❌ FAILED: Check the logs above for details")
        print("💡 Common fixes:")
        print("   1. Verify your .env file has correct API key and DB password")
        print("   2. Make sure PostgreSQL is running")
        print("   3. Check your internet connection")