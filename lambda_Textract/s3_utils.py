import boto3

def write_to_s3(bucket, key, data, content_type='text/plain'):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
