from zipfile import ZipFile
from datetime import datetime
import re
import requests
import os
import pandas as pd
import numpy as np
import json
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='airflow')
connection = mysql_hook.get_conn()
cursor = connection.cursor()


dir_name = os.path.dirname(os.path.abspath(__file__))
downloaded_path = os.path.join(dir_name, 'download_files')

download_urls = [ "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip", 
                 "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip", 
                 "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip", 
                 "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip"
]

api_url = "https://api.covidtracking.com/v2/us/daily.json"

def get_s3_file_name(url):
    name = re.findall('(?<=s3.amazonaws.com\/).*', url)
    return name[0]
    
def handle_zip_file(url):
    file_path = os.path.join(downloaded_path, get_s3_file_name(url))
    try:
        r = requests.get(url, allow_redirects=True)
        open(file_path, 'wb').write(r.content) 
        with ZipFile(file_path, 'r') as zip:
            # extracting all the files
            print('Extracting all the files now...')
            zip.extractall(downloaded_path)
            print('Done!')
    except Exception as e:
        print(e)
    finally:
        if os.path.isfile(file_path):
            os.remove(file_path)

def init_db(cursor):
    query = """
                DROP DATABASE IF EXISTS airflow;
                CREATE DATABASE airflow;
                USE airflow;

                CREATE TABLE stations (
                    station_id INT,
                    station_name VARCHAR(255),
                    PRIMARY KEY (station_id)
                );
                
                CREATE TABLE trips (
                    trip_id INT,
                    start_time DATETIME,
                    end_time DATETIME,
                    bikeid INT,
                    tripduration INT,
                    from_station_id INT,
                    to_station_id INT, 
                    usertype VARCHAR(20),
                    gender VARCHAR(10),
                    birthyear INT,
                    CONSTRAINT PK_trip PRIMARY KEY (trip_id, from_station_id, to_station_id),
                    FOREIGN KEY (from_station_id) REFERENCES stations(station_id),
                    FOREIGN KEY (to_station_id) REFERENCES stations(station_id)
                );
            """
    cursor.execute(query)

def handle_csv_file(df):
    #convert nan values to None (mysql error)
    df = df.replace({np.nan: None})
    #convert float to int
    df['tripduration'] = df['tripduration'].apply(lambda x: int(float(re.sub(',', '', str(x)))))
    print('int success')
    #convert string to datetime
    df['start_time'] = pd.to_datetime(df['start_time'], format='%Y-%m-%d %H:%M:%S')
    df['end_time'] = pd.to_datetime(df['end_time'], format='%Y-%m-%d %H:%M:%S')
    print('datetime success')
    #split to 2 tables
    trips = df[['trip_id', 'start_time', 'end_time', 'bikeid', 'tripduration', 'from_station_id',
                'to_station_id', 'usertype', 'gender', 'birthyear']]            
    stations = pd.concat([df[['from_station_id', 'from_station_name']].rename(columns={'from_station_id': 'station_id', 'from_station_name': 'station_name'}),
                        df[['to_station_id', 'to_station_name']].rename(columns={'to_station_id': 'station_id', 'to_station_name': 'station_name'})],
                        ignore_index=True,
                        axis=0)
    print('split success')
    #remove duplicates
    trips = trips.drop_duplicates(subset=['trip_id', 'from_station_id', 'to_station_id'])
    stations = stations.drop_duplicates(subset=['station_id', 'station_name'])
    print(stations.head(5))
    print(trips.head(5))
    print('remove success')
    return trips, stations

def upload_to_sql(cursor, trips, stations):
    for i,row in stations.iterrows():
        #here %S means string values 
        sql = 'INSERT INTO stations VALUES (%s,%s);'
        cursor.execute(sql, tuple(row))
    print("stations inserted")
    
    for i,row in trips.iterrows():
        #here %S means string values 
        sql = 'INSERT INTO trips VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
        cursor.execute(sql, tuple(row))
    print("trips inserted")
    
def upload_to_db(folder_path, cursor):
    arr = []
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            arr.append(os.path.join(folder_path, file))
    print(arr)
    # df = pd.DataFrame()
    try:
        for file in arr:
            df = pd.read_csv(file)
            trips, stations = handle_csv_file(df)
            print(trips)
            upload_to_sql(cursor, trips, stations)
    except Exception as e:
        print(e)
        
def get_output(cursor):
    try:
        sql = """
                USE airflow;
                
                SELECT COUNT(DISTINCT(t.bike_id)) AS count, s.station_name
                FROM trips t
                JOIN stations s
                ON t.from_station_id = s.station_id
                GROUP BY s.station_id
                ORDER BY COUNT(DISTINCT(t.bike_id)) DESC
                LIMIT 10;
            """
        cursor.execute(sql)
        a1 = cursor.fetchall()
        print('Top 10 from station \n')
        print(a1)
        print('\n')
        sql = """
                USE airflow;
                
                SELECT COUNT(DISTINCT(t.bike_id)) AS count, s.station_name
                FROM trips t
                JOIN stations s
                ON t.to_station_id = s.station_id
                GROUP BY s.station_id
                ORDER BY COUNT(DISTINCT(t.bike_id)) DESC
                LIMIT 10;
            """
        cursor.execute(sql)
        a2 = cursor.fetchall()
        print('Top 10 to station \n')
        print(a2)
        print('\n')
        sql = """
                USE airflow;
                
                SELECT COUNT(bike_id) AS count
                FROM trips 
                WHERE DATE(start_time) = '2019-05-16';
            """
        cursor.execute(sql)
        a3 = cursor.fetchall()
        print('New bike rentals \n')
        print(a3)
        print('\n')
        sql = """
                USE airflow;
                
                SELECT SUM(t.tripduration) AS sum_trip, s.station_name
                FROM trips t
                JOIN stations s
                ON t.from_station_id = s.station_id
                WHERE DATE(t.start_date) <= '2019-05-16'
                GROUP BY s.station_id
                LIMIT 10;
            """
        cursor.execute(sql)
        a4 = cursor.fetchall()
        print('Running totals \n')
        print(a4)
        print('\n')
        sql = """
                USE airflow;
                
                SELECT
                curr.date,
                curr.rentals curr_rentals,
                (cur.rentals - prev.rentals) change
                FROM (SELECT DATE(start_time) AS date, SUM(COUNT(bike_rentals)) rentals FROM trips GROUP BY DATE(start_time)) curr
                LEFT JOIN (SELECT DATE(start_time) AS date, SUM(COUNT(bike_rentals)) rentals FROM trips GROUP BY DATE(start_time)) prev
                ON prev.date = cur.date - INTERVAL 1 DAY;
            """
        cursor.execute(sql)
        a5 = cursor.fetchall()
        print('DoD \n')
        print(a5)
        print('\n')
    except Exception as e:
        print(e)


def get_api_from_url(url):
    print('mongo_success')
    try:
        response = json.loads(requests.get(url).text)
        meta = response['meta']
        data = response['data']
    except Exception as e:
        print(e)
        meta = {}
        data = {}
    return meta, data
    
def upload_to_mongo(url):
    
    mongo_conn = 'mongodb://airflow:airflow@172.20.0.1:27017/'
    mongo_client = MongoClient(mongo_conn)
    mongodb = mongo_client['hometest']
    meta, data = get_api_from_url(url)
    print(f"Connected to MongoDB - {mongo_client.server_info()}")
    try:
        
        meta_collection =  mongodb.get_collection('meta')
        if meta_collection.drop():
            pass
        else:
            meta_collection.insert_one(meta)
        
        data_collection = mongodb.get_collection('data')
        if data_collection.drop():
            pass
        else:
            data_collection.insert_many(data)
    except Exception as e:
        print(e)

def check_mongo_api():
    pass

with DAG('SQL_query', description='Hello World DAG',
          schedule_interval='@once',
          start_date=datetime(2017, 3, 20), catchup=False) as dag:
    
    # create_db = PythonOperator(task_id='create_db',
    #                            python_callable=init_db,
    #                            op_kwargs={'cursor': cursor})
    
    # csv_to_db =  PythonOperator(task_id='upload_to_db', 
    #                            python_callable=upload_to_db, 
    #                            op_kwargs={'folder_path': downloaded_path,
    #                                    'cursor': cursor})
    queries = PythonOperator(task_id='get_output',
                             python_callable=get_output,
                             op_kwargs={'cursor': cursor})
    
    # i = 0
    # for url in download_urls:
    #     zip_to_csv = PythonOperator(task_id=f'zip_to_csv_{i}', 
    #                               python_callable=handle_zip_file,
    #                               op_kwargs={'url':url})
    #     i += 1
    #     create_db >> zip_to_csv >> csv_to_db >> queries
        
    # api_to_mongo = PythonOperator(task_id='api_to_mongo',
    #                          python_callable=upload_to_mongo,
    #                          op_kwargs={'url': api_url}
    #                                     )
    # api_to_mongo