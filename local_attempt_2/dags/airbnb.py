from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash import BashOperator
from random import randint
#from datetime import datetime
from datetime import datetime
from datetime import date
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

# connect to postgres db
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

default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule_interval="@daily", start_date=datetime(2021, 1,1), catchup=False)
def taskflow_airbnb2():
    


    @task()
    def extract_transform_load_weather_data():
        # Source weather_boston: https://www.kaggle.com/jqpeng/boston-weather-data-jan-2013-apr-2018
        weather_boston = "./data/Boston/Boston weather_clean.csv"
        # Source weather_seattle: https://github.com/plotly/datasets/blob/master/2016-weather-data-seattle.csv
        weather_seattle = "./data/Seattle/2016-weather-data-seattle.csv"
        # Source weather_seattle2: https://www.kaggle.com/rtatman/did-it-rain-in-seattle-19482017
        weather_seattle2 = "./data/Seattle/seattleWeather_1948-2017.csv"

        #weather_seattle_two_df = pd.read_csv(weather_seattle2) \
        #    .to_csv(weather_seattle2, quoting=csv.QUOTE_NONE, index=False)
        
        # Seattle data:  
        weather_seattle_two_df = pd.read_csv(weather_seattle2)
        # convert from fahrenheit to celsius, count average temp
        weather_seattle_two_df["avg_temp_in_celsius"] = ((weather_seattle_two_df["TMAX"] + weather_seattle_two_df["TMIN"]) /2 -32) * 5/9
        # filter out unnecessary columns
        weather_seattle_two_df = weather_seattle_two_df[["DATE","avg_temp_in_celsius"]]
        weather_seattle_two_df['DATE'] = pd.to_datetime(weather_seattle_two_df['DATE'])
        # limit to timerange between 2016 - 2018
        weather_seattle_two_df = weather_seattle_two_df.loc[(weather_seattle_two_df['DATE'] >= "2016") & (weather_seattle_two_df['DATE'] <= "2018")]
        # rename columns to be able to concatenate data  with Boston-data later
        weather_seattle_two_df = weather_seattle_two_df.rename(columns={"DATE": "date"})
        #add Seattle "label"
        weather_seattle_two_df["city"] = "Seattle"


        #Boston data:

        #transform Boston weather data
        weather_boston_df = pd.read_csv(weather_boston)
        # generate date column for Boston-time date information which is saved among 3 columns
        weather_boston_df['date']= weather_boston_df.apply(lambda x:datetime.strptime("{0} {1} {2}".format(x['Year'],x['Month'], x['Day']), "%Y %m %d"),axis=1)
        # filter out unnecessary columns
        weather_boston_df = weather_boston_df[['date','Avg Temp (F)']]
        #limit to time range between 2016 - 2018
        weather_boston_df = weather_boston_df.loc[(weather_boston_df['date'] >= "2016") & (weather_boston_df['date'] <= "2018")]

        #convert fahrenheit values to celsius
        weather_boston_df["Avg Temp (F)"] = weather_boston_df.apply(lambda x: (x['Avg Temp (F)']-32) * 5/9, axis=1)
        #rename columns to concatenate data with Seattle-data
        weather_boston_df = weather_boston_df.rename(columns={"Avg Temp (F)": "avg_temp_in_celsius"})
        #Add Boston label
        weather_boston_df["city"] = "Boston"

        # concatenate Boston and Seattle data to get one single dataframe for weather data
        weather_df = pd.concat([weather_boston_df,weather_seattle_two_df])
        weather_df.reset_index(inplace=True)

        #load
        write_in_db(weather_df,"weather")

    
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

        listings_boston = "./data/Boston/listings.csv"
        listings_seattle = "./data/Seattle/listings.csv"
        # read listings data and filter out unnecessary columns
        listings_boston_df = pd.read_csv(listings_boston)
        listings_boston_df = listings_boston_df[selected_listings_attributes]
        listings_seattle_df = pd.read_csv(listings_seattle)
        listings_seattle_df = listings_seattle_df[selected_listings_attributes]
        # concatenate Boston and Seattle dataframe for listings
        listings_df = pd.concat([listings_boston_df,listings_seattle_df])
        listings_df.reset_index(inplace=True)

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


        return 0

    @task()
    def transform_listings(listings_df :json):
        listings_df = pd.read_json(listings_df)

        return listings_df.to_json()



    @task()
    def load_into_data_warehouse_listings(listings_df_cleansed :json):
        listings_df_cleansed = pd.read_json(listings_df_cleansed)
        write_in_db(listings_df_cleansed, "listings")
        

    @task()
    def load_from_data_lake_to_data_warehouse_reviews():
        df = pd.read_csv("./data/transformed_reviews.csv", lineterminator="\n")
        # filter out unnecessary columns
        df = df[['listing_id','id','date','reviewer_id','reviewer_name','sentiments']]
        df['date'] = pd.to_datetime(df['date'])
        #df = df.loc[(df['date'] >= "2016") & (df['date'] <= "2018")]

        # load to dw
        write_in_db(df, "reviews")


    # define data marts:

    @task()
    def load_into_data_mart_avg_price_neighborhood_state():
        #transform price into a countable format: remove $-sign and commas 
        # Calculate aggregated average price of listings to neigborhood
        
        df = pd.read_sql_query("SELECT avg(cast(replace(replace(price,'$',''),',','') as float)) as avg_price_in_usd, neighbourhood_group_cleansed, market as city FROM public.listings market where (market ='Boston' or market ='Seattle') group by neighbourhood_group_cleansed, market",con=engine)
        write_in_db(df, "data_mart_avg_price_neighborhood_state")

    @task()
    def load_into_data_mart_avg_price_city():
        #transform price into a countable format: remove $-sign and commas 
        # Calculate aggregated average price of listings to market (city)
        
        df = pd.read_sql_query("SELECT avg(cast(replace(replace(price,'$',''),',','') as float)) as avg_price_in_usd, market FROM public.listings where market ='Boston' or market ='Seattle' or market = 'San Francisco' group by market",con=engine)
        write_in_db(df, "data_mart_avg_price_city")

    @task()
    def load_into_data_mart_score_for_bathrooms_bedrooms_ratio():
        # This analysis carries out which bathroom/bedroom-ratio for listings scores best.
        # the score is calculated with 'review_scores_rating * number_of_reviews'
        # the final result shows a ranking of the 'average score' for the several 'ratios'
        
        df = pd.read_sql_query("select ag.bathrooms_bedrooms_ratio, avg(ag.score) as average_score from ( SELECT bathrooms / bedrooms as bathrooms_bedrooms_ratio, review_scores_rating * number_of_reviews as score FROM public.listings where bedrooms is not null and bedrooms != 0 and bathrooms is not null and review_scores_rating is not null and number_of_reviews is not null order by bathrooms_bedrooms_ratio desc ) as ag group by ag.bathrooms_bedrooms_ratio",con=engine)
        write_in_db(df, "data_mart_score_for_bathrooms_bedrooms_ratio")

    @task()
    def load_into_data_mart_rising_used_listings_over_time():
        

        df = pd.read_sql_query("select count(listing_id) as amount_of_listings, cast(calendar.date as date) as date from calendar where available = 'f'  and date > '2016-09-05' and date < '2017-01-03' group by date",con=engine)
        df['date'] = pd.to_datetime(df['date'])
        write_in_db(df, "data_mart_rising_used_listings_over_time")

    @task()
    def load_into_data_mart_rising_used_listings_in_neighborhoods_over_time():
        df = pd.read_sql_query("select neighbourhood_group_cleansed, cast(calendar.date as date) as date, count(neighbourhood_group_cleansed) as c_neighborhood from listings inner join calendar on listings.id = calendar.listing_id where available = 'f' and neighbourhood_group_cleansed is not null group by date, neighbourhood_group_cleansed",con=engine)
        df['date'] = pd.to_datetime(df['date'])
        write_in_db(df, "data_mart_rising_used_listings_in_neighborhoods_over_time")


    @task()
    def load_into_data_mart_performance_apartments_houses():
        # measure for performance: amount of listings that were not available at a point of time
        df = pd.read_sql_query("select property_type, cast(calendar.date as date) as date, count(*) as amount_of_listings from listings inner join calendar on listings.id = calendar.listing_id where available = 'f'  and date > '2016-09-05' and date < '2017-01-03' group by date, property_type",con=engine)
        df['date'] = pd.to_datetime(df['date'])
        write_in_db(df, "data_mart_performance_apartments_houses")
        return 0

    @task()
    def load_into_data_mart_performance_measured_by_weather_in_both_cities():
        # measure for performance: amount of listings that were not available at a point of time
        df = pd.read_sql_query("select cast(calendar.date as date) as date_calendar, weather.city, count(listings.id) as amount_of_listings, weather.avg_temp_in_celsius from calendar inner join weather on weather.date = cast(calendar.date as date) inner join listings on listings.id = calendar.listing_id where available = 'f' and listings.market = weather.city and weather.date > '2016-09-05' and weather.date < '2017-01-03' group by date_calendar, weather.city, avg_temp_in_celsius",con=engine)
        df['date_calendar'] = pd.to_datetime(df['date_calendar'])
        write_in_db(df, "data_mart_performance_measured_by_weather_in_both_cities")
        return 0

    @task()
    def load_into_data_mart_development_of_avg_price_in_both_cities():
        df = pd.read_sql_query("select cast(calendar.date as date) as date, avg(cast(replace(replace(calendar.price,'$',''),',','') as float)) as avg_price_in_usd, market as city from calendar inner join listings on calendar.listing_id  = listings.id where available = 't' and (market ='Boston' or market ='Seattle') group by date, market", con=engine)
        df['date'] = pd.to_datetime(df['date'])
        write_in_db(df, "data_mart_development_of_avg_price_in_both_cities")
        return 0

    @task()
    def load_into_data_mart_sentiment_analysis():
        # sentiments aggregated by market (city)
        # sentiments are counted per date and city
        df = pd.read_sql_query("SELECT reviews.date, sentiments, count(*), market as amount from reviews inner join listings on listings.id = reviews.listing_id where market ='Boston' or market ='Seattle' group by  reviews.date, sentiments, market", con=engine)
        df['date'] = pd.to_datetime(df['date'])
        write_in_db(df, "data_mart_sentiment_analysis")
        return 0 

    #set up the taskflow:
    extract_listings = extract_listings()
    extract_and_load_into_data_warehouse_calendar = extract_and_load_into_data_warehouse_calendar()
    transform_listings = transform_listings(extract_listings)
    load_into_data_warehouse_listings = load_into_data_warehouse_listings(transform_listings)
    load_from_data_lake_to_data_warehouse_reviews = load_from_data_lake_to_data_warehouse_reviews()
    
    load_into_data_mart_sentiment_analysis = load_into_data_mart_sentiment_analysis()
    load_into_data_mart_avg_price_neighborhood_state = load_into_data_mart_avg_price_neighborhood_state()
    load_into_data_mart_avg_price_city = load_into_data_mart_avg_price_city()
    load_into_data_mart_score_for_bathrooms_bedrooms_ratio = load_into_data_mart_score_for_bathrooms_bedrooms_ratio()
    load_into_data_mart_rising_used_listings_over_time = load_into_data_mart_rising_used_listings_over_time()
    load_into_data_mart_rising_used_listings_in_neighborhoods_over_time = load_into_data_mart_rising_used_listings_in_neighborhoods_over_time()
    load_into_data_mart_performance_measured_by_weather_in_both_cities = load_into_data_mart_performance_measured_by_weather_in_both_cities()
    load_into_data_mart_development_of_avg_price_in_both_cities = load_into_data_mart_development_of_avg_price_in_both_cities()

    load_into_data_mart_performance_apartments_houses = load_into_data_mart_performance_apartments_houses()
    extract_transform_load_weather_data = extract_transform_load_weather_data()

    extract_listings >>  transform_listings >> load_into_data_warehouse_listings >> [load_into_data_mart_avg_price_city, load_into_data_mart_avg_price_neighborhood_state, load_into_data_mart_score_for_bathrooms_bedrooms_ratio, load_into_data_mart_rising_used_listings_over_time, load_into_data_mart_rising_used_listings_in_neighborhoods_over_time, load_into_data_mart_performance_apartments_houses, load_into_data_mart_performance_measured_by_weather_in_both_cities, load_into_data_mart_development_of_avg_price_in_both_cities]
    extract_and_load_into_data_warehouse_calendar >> [load_into_data_mart_avg_price_city, load_into_data_mart_avg_price_neighborhood_state, load_into_data_mart_score_for_bathrooms_bedrooms_ratio, load_into_data_mart_rising_used_listings_over_time, load_into_data_mart_rising_used_listings_in_neighborhoods_over_time, load_into_data_mart_performance_apartments_houses, load_into_data_mart_performance_measured_by_weather_in_both_cities, load_into_data_mart_development_of_avg_price_in_both_cities]
    extract_transform_load_weather_data
    load_from_data_lake_to_data_warehouse_reviews >> load_into_data_mart_sentiment_analysis
    

taskflow_airbnb2 = taskflow_airbnb2()
taskflow_airbnb2