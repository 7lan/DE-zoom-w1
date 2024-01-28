import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import argparse


def main(params):
    
    pd.set_option('display.max_columns', 50)
    
    username = params.username
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    
    os.system('wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz -O green_taxi_trips.csv.gz')
    os.system('wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O taxi_zones.csv')

    trips = pd.read_csv('green_taxi_trips.csv.gz', iterator=True, chunksize=100000)
    zones = pd.read_csv('taxi_zones.csv')
    trip_chunk = next(trips)

    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
    print(engine)
    con = engine.connect()

    trip_chunk['lpep_pickup_datetime'] = pd.to_datetime(trip_chunk['lpep_pickup_datetime'])
    trip_chunk['lpep_dropoff_datetime'] = pd.to_datetime(trip_chunk['lpep_dropoff_datetime'])
    
    print(trip_chunk.head())
    print(zones.head())

    zones.to_sql('ny_zones', con=engine, if_exists='replace', index=False)
    trip_chunk.to_sql('taxi_trips', con=engine, if_exists='replace', index=False)

    while True:
        try:
            trip_chunk = next(trips)
        except StopIteration as e:
            print(e)
            break
            

        trip_chunk['lpep_pickup_datetime'] = pd.to_datetime(trip_chunk['lpep_pickup_datetime'])
        trip_chunk['lpep_dropoff_datetime'] = pd.to_datetime(trip_chunk['lpep_dropoff_datetime'])

        trip_chunk.to_sql('taxi_trips', con=engine, if_exists='append', index=False)

        print('done with another 100K')
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='parser to get db and info etc.')
    parser.add_argument('-username', help="for username")
    parser.add_argument('-password', help="for username")
    parser.add_argument('-host', help="for username")
    parser.add_argument('-port', help="for username")
    parser.add_argument('-database', help="for username")    
    
    args = parser.parse_args()
    main(args)