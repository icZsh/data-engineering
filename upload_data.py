import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def upload_data(params):
    """
    Upload CSV data to PostgreSQL in chunks.
    
    Args:
        params: Parsed command line arguments containing database credentials and file info
    Returns:
        bool: True if upload successful, False otherwise
    """
    try:
        # Validate file existence
        if not os.path.exists(params.csv_name):
            raise FileNotFoundError(f"CSV file {params.csv_name} not found")

        # Create database URL safely
        db_url = f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}'
        engine = create_engine(db_url)
        
        # Get total rows for progress tracking
        total_rows= sum(1 for _ in open(params.csv_name)) - 1
        print(f"Starting upload of {params.csv_name} ({total_rows:,} rows)...")
        
        df_iter = pd.read_csv(params.csv_name, iterator=True, chunksize=100000)
        processed_rows = 0
        
        for i, df in enumerate(df_iter):
            chunk_start = time()
            
            # Convert datetime columns more efficiently
            datetime_columns = [col for col in df.columns if 'datetime' in col.lower()]
            for col in datetime_columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Handle NULL values
            df = df.replace({pd.NA: None})
            
            # Upload to postgres with appropriate index
            df.to_sql(
                name=params.table_name,
                con=engine,
                if_exists='replace' if i == 0 else 'append',
                index=False,
                method='multi'  # Faster inserts
            )
            
            # Log progress with percentage
            processed_rows += len(df)
            percentage = (processed_rows / total_rows) * 100
            elapsed = time() - chunk_start
            speed = len(df) / elapsed if elapsed > 0 else 0
            
            print(f"Chunk {i+1}: {processed_rows:,}/{total_rows:,} rows "
                  f"({percentage:.1f}%) processed at {speed:.0f} rows/sec")
        
        print(f"\nSuccessfully uploaded {processed_rows:,} rows to {params.table_name}")
        return True

    except Exception as e:
        print(f"Error: {str(e)}")
        return False
        
    finally:
        if 'engine' in locals():
            engine.dispose()

def main():
    parser = argparse.ArgumentParser(description='Ingest CSV data to PostgreSQL database')
    parser.add_argument('--user', required=True, help='PostgreSQL username')
    parser.add_argument('--password', required=True, help='PostgreSQL password')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', default='5432', help='PostgreSQL port')
    parser.add_argument('--db', required=True, help='PostgreSQL database name')
    parser.add_argument('--table_name', required=True, help='Target table name')
    parser.add_argument('--csv_name', required=True, help='Input CSV file path')
    
    args = parser.parse_args()
    success = upload_data(args)
    exit(0 if success else 1)

if __name__ == "__main__":
    main()