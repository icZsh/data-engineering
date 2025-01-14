import pandas as pd
from sqlalchemy import create_engine
from time import time

#read csv and simple data transformation
df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)

#create engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

while True:
    start_time = time()
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists = 'append')
    end_time = time()
    print("Inserted completed, it took %.3f second" % (end_time - start_time))

#get schema
# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))