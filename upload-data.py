import pandas as pd
from sqlalchemy import create_engine
from time import time


def upload_data(csv_path: str, table_name: str, chunk_size: int = 100000):
    """Upload CSV data to PostgreSQL in chunks."""
    
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    df_iter = pd.read_csv(csv_path, iterator=True, chunksize=chunk_size)
    
    total_rows = 0
    try:
        print(f"Start uploading {csv_path} ...")
        for i, df in enumerate(df_iter):
            start = time()
            
            # Convert date columns
            for col in df.columns:
                 if 'datetime' in col:
                    df[col] = pd.to_datetime(df[col])
            
            # Upload to postgres
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace' if i == 0 else 'append',
                index=False
            )
            
            # Log progress
            total_rows += len(df)
            print(f"Inserted chunk {i+1}: {total_rows:,} rows ({time() - start:.2f} sec)")
    
    except Exception as e:
        print(f"Error uploading {csv_path}: {e}")
    finally:
        engine.dispose()

    print(f"{csv_path} is uploaded successfully.")

upload_data('green_tripdata_2019-10.csv', 'green_tripdata')
upload_data('taxi_zone_lookup.csv', 'taxi_zone_lookup')