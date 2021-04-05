from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime
import pandas as pd
import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago



import json
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pathlib

from sqlalchemy import create_engine
import psycopg2 
import io
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule_interval="@daily", start_date=datetime(2021, 1,1), catchup=False, tags=['example'])
def taskflow_airbnb2():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
    def write_in_db(df,table_name):
        df.head(0).to_sql(table_name, engine, if_exists='replace',index=False) #drops old table and creates new empty table
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        cur.copy_from(output, table_name, null="") # null values become ''
        conn.commit()
        conn.close()
        return 0

    @task()
    def extract_listings():
        selected_listings_attributes = [
            'id',
            'price',
            'neighbourhood_group_cleansed',
            'city',
            'state',
            'market',
            'country',
            'property_type',
            'room_type',
            'accommodates',
            'bathrooms',
            'bedrooms',
            'beds',
            'bed_type',
            'square_feet',
            'minimum_nights',
            'maximum_nights',
            'number_of_reviews',
            'review_scores_rating'
             ]
        print(pathlib.Path().absolute())
        #os.path.abspath("mydir/myfile.txt")
        listings_boston = "./data/Boston/listings.csv"
        listings_seattle = "./data/Seattle/listings.csv"
        listings_boston_df = pd.read_csv(listings_boston)
        listings_boston_df = listings_boston_df[selected_listings_attributes]
        listings_seattle_df = pd.read_csv(listings_seattle)
        listings_seattle_df = listings_seattle_df[selected_listings_attributes]
        listings_df = pd.concat([listings_boston_df,listings_seattle_df])
        listings_df.reset_index(inplace=True)
      #  file_task = FileSensor(task_id="check_file", filepath=data_file)
        return listings_df.to_json()

    @task()
    def extract_and_load_into_data_warehouse_calendar():
        
        calendar_boston = "./data/Boston/calendar.csv"
        calendar_seattle = "./data/Seattle/calendar.csv"
        calendar_boston_df = pd.read_csv(calendar_boston)
        calendar_seattle_df = pd.read_csv(calendar_seattle)
        calendar_df = pd.concat([calendar_boston_df,calendar_seattle_df])
        calendar_df.reset_index(inplace=True)

        write_in_db(calendar_df,"calendar")
      #  file_task = FileSensor(task_id="check_file", filepath=data_file)

        return 0

    @task()
    def transform(listings_df :json):
        listings_df = pd.read_json(listings_df)
        #some cleansing task if needed
        #listings_df_cleansed = listings_df.dropna()
        #dict_you_want = { your_key: old_dict[your_key] for your_key in your_keys }
        return listings_df.to_json()



    @task()
    def load_into_data_warehouse(listings_df_cleansed :json):
        listings_df_cleansed = pd.read_json(listings_df_cleansed)
        write_in_db(listings_df_cleansed, "listings")
        
    @task()
    def load_into_data_mart_neighborhood():
        #transform price into a countable format: remove $-sign and commas 
        # Calculate aggregated average price of listings to localities like neigborhood, market, city and state
        
        df = pd.read_sql_query("SELECT avg(cast(replace(replace(price,'$',''),',','') as float)) as avg_price, neighbourhood_group_cleansed, city, market, state FROM public.listings group by neighbourhood_group_cleansed, city, market, state  ;",con=engine)
        write_in_db(df, "data_mart_neighborhood")


    @task()
    def load_into_data_mart_score_for_bathrooms_bedrooms_ratio():
        # This analysis carries out which bathroom/bedroom-ratio for listings scores best.
        # the score is calculated with 'review_scores_rating * number_of_reviews'
        # the final results shows a ranking of the 'average score' for the several 'ratios'
        
        df = pd.read_sql_query("select ag.bathrooms_bedrooms_ratio, avg(ag.score) as average_score from ( SELECT bathrooms / bedrooms as bathrooms_bedrooms_ratio, review_scores_rating * number_of_reviews as score FROM public.listings where bedrooms is not null and bedrooms != 0 and bathrooms is not null and review_scores_rating is not null and number_of_reviews is not null order by bathrooms_bedrooms_ratio desc ) as ag group by ag.bathrooms_bedrooms_ratio",con=engine)
        write_in_db(df, "data_mart_score_for_bathrooms_bedrooms_ratio")

    @task()
    def load_into_data_mart_rising_used_listings_over_time():
        

        df = pd.read_sql_query("select count(listing_id), cast(calendar.date as date) as date from calendar where available = 't' group by date ",con=engine)
        write_in_db(df, "data_mart_rising_used_listings_over_time")

    @task()
    def load_into_data_mart_rising_used_listings_in_neighborhoods_over_time():
        df = pd.read_sql_query("select neighbourhood_group_cleansed, cast(calendar.date as date) as date, count(neighbourhood_group_cleansed) as c_neighborhood from listings inner join calendar on listings.id = calendar.listing_id where available = 't' and neighbourhood_group_cleansed is not null group by date, neighbourhood_group_cleansed",con=engine)
        write_in_db(df, "data_mart_rising_used_listings_in_neighborhoods_over_time")

    #taskflow:
    extract_listings = extract_listings()
    extract_and_load_into_data_warehouse_calendar = extract_and_load_into_data_warehouse_calendar()
    transform = transform(extract_listings)
    load_into_data_warehouse = load_into_data_warehouse(transform)
    load_into_data_mart_neighborhood = load_into_data_mart_neighborhood()
    load_into_data_mart_score_for_bathrooms_bedrooms_ratio = load_into_data_mart_score_for_bathrooms_bedrooms_ratio()
    load_into_data_mart_rising_used_listings_over_time = load_into_data_mart_rising_used_listings_over_time()
    load_into_data_mart_rising_used_listings_in_neighborhoods_over_time = load_into_data_mart_rising_used_listings_in_neighborhoods_over_time()

    extract_listings >>  transform >> load_into_data_warehouse >> [load_into_data_mart_neighborhood, load_into_data_mart_score_for_bathrooms_bedrooms_ratio, load_into_data_mart_rising_used_listings_over_time, load_into_data_mart_rising_used_listings_in_neighborhoods_over_time]
    extract_and_load_into_data_warehouse_calendar >> [load_into_data_mart_neighborhood, load_into_data_mart_score_for_bathrooms_bedrooms_ratio, load_into_data_mart_rising_used_listings_over_time, load_into_data_mart_rising_used_listings_in_neighborhoods_over_time]
taskflow_airbnb2 = taskflow_airbnb2()
taskflow_airbnb2