import logging
import os
import shutil
from datetime import datetime, timedelta

import boto3
import emoji
import pandas as pd
from googleapiclient.discovery import build
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, udf
from pyspark.sql.types import (DateType, IntegerType, LongType, StringType, StructField, StructType)

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 3, 26, 0, 0, 0)
}

dag = DAG(
    dag_id='youtube_etl_dag',
    default_args=default_args,
    description = 'A simple ETL DAG',
    schedule_interval='@daily',
    catchup=False)

#Function to extract data from YT Api
def extract_data(**kwargs):
    api_key = kwargs['api_key']
    region_codes = kwargs['region_codes']
    category_ids = kwargs['category_ids']

    df_trending_video = fetch_data(api_key, region_codes, category_ids)
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_path = f'/opt/airflow/Youtube_Trending_Data_Raw_{current_date}'
    df_trending_video.to_csv(output_path, index=False)

def fetch_data(api_key, region_codes, category_ids):
    #initialize an empty list to hold data
    video_data = []

    #Build YT API service
    youtube = build('youtube', 'v3', developerKey=api_key)

    for region_code in region_codes:
        for category_id in category_ids:
            next_page_token = None
            while True:
                request = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    chart = 'mostPopular',
                    regionCode = region_code,
                    videoCategoryId = category_id,
                    maxResults = 50,
                    pageToken = next_page_token)
                response = request.execute()
                videos = response['items']

                for video in videos:
                    video_info = {'region_code': region_code,
                                  'category_id': category_id,
                                  'video_id': video['id'],
                                  'title': video['snippet']['title'],
                                  'published_at': video['snippet']['publishedAt'],
                                  'view_count': int(video['statistics'].get('viewCount', 0)),
                                  'like_count': int(video['statistics'].get('likeCount', 0)),
                                  'comment_count': int(video['statistics'].get('commentCount', 0)),
                                  'channel_title': video['snippet']['channelTitle']
                                  }
                    video_data.append(video_info)

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
    return pd.DataFrame(video_data)


extract_task = PythonOperator(task_id='extract_data_from_youtube_api',
                              python_callable = extract_data,
                              op_kwargs={
                                  'api_key': os.getenv('YOUTUBE_API_KEY'),
                                  'region_codes': ['US', 'GB','PL', 'AU', 'NZ'],
                                  'category_ids': ['1', '2', '10', '15', '20', '22', '23']
                              },
                              dag=dag,
                              )

extract_task