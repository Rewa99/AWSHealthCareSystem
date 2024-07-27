import boto3 #type:ignore
import csv

s3 = boto3.client('s3')

def get_s3_object(bucket_name, object_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        body_bytes = response['Body'].read()
        body_string = body_bytes.decode('utf-8')
        return body_string
    except Exception as e:
        print(f"Error getting S3 object: {e}")
        raise

def get_questions_from_csv(bucket_name, object_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.reader(content)
        questions = [row[0] for row in reader if row]
        return questions
    except Exception as e:
        print(f"Error reading CSV: {e}")
        raise

def upload_csv_to_s3(bucket_name, object_key, data):
    try:
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=data.encode('utf-8'), ContentType='text/csv')
    except Exception as e:
        print(f"Error uploading CSV to S3: {e}")
        raise
