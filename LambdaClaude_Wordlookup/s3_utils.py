import boto3
import pandas as pd
from io import StringIO

s3 = boto3.client('s3')

def get_s3_object(bucket_name, object_key):
    try:
        print(f"Getting S3 Object: Bucket={bucket_name}, Key={object_key}")
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        body_bytes = response['Body'].read()
        body_string = body_bytes.decode('utf-8')
        return body_string
    except Exception as e:
        print(f"Error getting S3 object: {e}")
        raise

def read_questions(bucket_name, file_path):
    print(f"Reading Questions from: {file_path}")
    response = s3.get_object(Bucket=bucket_name, Key=file_path)
    content = response['Body'].read().decode('utf-8')
    questions = pd.read_csv(StringIO(content), header=None, names=['Question', 'Sr.No'])

    # Debug print to check column names
    print(f"Questions DataFrame Columns: {questions.columns.tolist()}")

    return questions

def read_keywords(bucket_name, file_path):
    print(f"Reading Keywords from: {file_path}")
    response = s3.get_object(Bucket=bucket_name, Key=file_path)
    content = response['Body'].read().decode('utf-8')
    keywords_df = pd.read_csv(StringIO(content), header=None, names=['Sr.No', 'Keywords'])

    # Debug print to check column names
    print(f"Keywords DataFrame Columns: {keywords_df.columns.tolist()}")

    return keywords_df

def upload_csv_to_s3(bucket_name, object_key, data):
    print(f"Uploading CSV to: Bucket={bucket_name}, Key={object_key}")
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=data.encode('utf-8'), ContentType='text/csv')
