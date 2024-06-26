import boto3

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

def upload_csv_to_s3(bucket_name, object_key, data):
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=data.encode('utf-8'), ContentType='text/csv')
