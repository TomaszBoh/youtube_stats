import logging
import os
import shutil
import re
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

def preprocess_data_pyspark_job():
    spark = SparkSession.builder.appName('YouTubeTransform').getOrCreate()
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_path = f'/opt/airflow/Youtube_Trending_Data_Raw_{current_date}'
    df = spark.read.csv(output_path, header=True)
    
    # Define UDF to remove hashtag data, emojis
    def clean_text(text):
     if text is not None:
        # Remove emojis
        text = emoji.demojize(text, delimiters=('', ''))
        
        # Remove hashtag data
        if text.startswith('#'):
            text = text.replace('#', '').strip()
        else:
            split_text = text.split('#')
            text = split_text[0].strip()
        
        # Remove extra double quotes and backslashes
        text = text.replace('\\"', '')  # Remove escaped quotes
        text = re.sub(r'\"+', '', text)  # Remove remaining double quotes
        text = text.replace('\\', '')  # Remove backslashes
        
        return text.strip()  # Strip any leading or trailing whitespace

     return text
    # Register UDF
    clean_text_udf = udf(clean_text, StringType())

    # Clean the data
    df_cleaned = df.withColumn('title', clean_text_udf(col('title'))) \
                   .withColumn('channel_title', clean_text_udf(col('channel_title'))) \
                   .withColumn('published_at', to_date(col('published_at'))) \
                   .withColumn('view_count', col('view_count').cast(LongType())) \
                   .withColumn('like_count', col('like_count').cast(LongType())) \
                   .withColumn('comment_count', col('comment_count').cast(LongType())) \
                   .dropna(subset=['video_id'])
    
    # Generate the filename based on the current date
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_path = f'/opt/airflow/Transformed_Youtube_Data_{current_date}'
    
    # Write cleaned DataFrame to the specified path
    df_cleaned.write.csv(output_path, header=True, mode='overwrite')

def load_data_to_s3(**kwargs):
    bucket_name = kwargs['bucket_name']
    today = datetime.now().strftime('%Y-%m-%d')
    prefix = f"processed-data/{today}"
    current_date = datetime.now().strftime("%Y-%m-%d")
    local_dir_path = f'/opt/airflow/Transformed_Youtube_Data_{current_date}'
    upload_to_s3(bucket_name,prefix,local_dir_path)

def upload_to_s3(bucket_name,prefix,local_dir_path):
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    s3_client = boto3.client(
        's3',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key
    )
    for root, dirs, files in os.walk(local_dir_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                s3_key = f"{prefix}/{file}"
                logging.info(f"Uploading {file_path} to S3://{bucket_name}/{s3_key}")
                s3_client.upload_file(file_path, bucket_name, s3_key)


#Aby miec dzialajace zadanie w DAG trzeba zdefiniować task

#Define extract task to DAG
extract_task = PythonOperator(task_id='extract_data_from_youtube_api',
                              python_callable = extract_data,
                              op_kwargs={
                                  'api_key': os.getenv('YOUTUBE_API_KEY'),
                                  'region_codes': ['US', 'GB','PL', 'AU', 'NZ'],
                                  'category_ids': ['1', '2', '10', '15', '20', '22', '23']
                              },
                              dag=dag,
                              )

#Define preprocessing task to DAG
preprocess_data_pyspark_task = PythonOperator(
    task_id = "preprocess_data_pyspark_task",
    python_callable = preprocess_data_pyspark_job,
    dag=dag
)

#Define Loadi to S3 Task to DAG
load_data_to_s3_task = PythonOperator(
    task_id = 'load_data_to_s3',
    python_callable = load_data_to_s3,
    op_kwargs = {'bucket_name': 'tb-youtube-trends-analysis-2025'},
    dag=dag

)

extract_task >> preprocess_data_pyspark_task >> load_data_to_s3_task