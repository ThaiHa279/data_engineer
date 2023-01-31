#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
from time import time
from sqlalchemy import create_engine
from pyarrow.parquet import ParquetFile
import pyarrow as pa
import argparse

def main(params): 
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    # download file data
    pf_name = 'output.parquet'
    os.system(f'wget {url} -O {pf_name}')
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    pf = ParquetFile(pf_name) 
    all_batch = pf.iter_batches(batch_size = 100000)
   
    batch = next(all_batch)
    df = pa.Table.from_batches([batch]).to_pandas()
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    pd.io.sql.get_schema(df , name=table_name, con = engine)
    df.head(n=0).to_sql(name=table_name, con = engine, if_exists='replace')
    df.to_sql(name=table_name, con = engine, if_exists='append')


    while True: 
        try:
            t_start = time()
                    
            batch = next(all_batch)
            df = pa.Table.from_batches([batch]).to_pandas()
            
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con = engine, if_exists='append')
            
            t_end = time()
            print('inserted another chunk, took %.3f second' %(t_end-t_start))
        except StopIteration: 
            print("Finished ingesting data into the postgres database. ")
            break
        
if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgrest')
    # user
    # password
    # host
    # port
    # database name
    # table name 
    # url of the parquet file  
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we wil write result to')
    parser.add_argument('--url', required=True, help='url of the parquet file')
    args = parser.parse_args()

    main(args)




