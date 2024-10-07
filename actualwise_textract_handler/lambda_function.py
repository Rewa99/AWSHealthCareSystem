import os
import json
import boto3
import time
from urllib.parse import unquote_plus


def lambda_handler(event, context):
    results = []
    textract = boto3.client('textract', region_name='us-east-1')

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        file_base_name = os.path.splitext(os.path.basename(key))[0]

        try:
            job_id = start_textract_job(textract, bucket, key)
            print(f"Started job with ID: {job_id}")

            # Wait for Textract to finish processing
            time.sleep(5)  # Initial wait
            pages = poll_textract(textract, job_id)
            print(f"Retrieved pages: {pages}")

            if not pages:
                raise Exception("No pages retrieved from Textract.")

            formatted_text = format_pages_with_numbers(pages)
            print("Formatted text: ", formatted_text)

            result_key = f"output/cleaned_input/cleaned_{file_base_name}.txt"
            write_to_s3(bucket, result_key, formatted_text)

            results.append({
                "Key": key,
                "JobId": job_id,
                "Status": "Completed",
                "OutputFile": result_key
            })
        except Exception as e:
            results.append({
                "Key": key,
                "Status": "Failed",
                "Reason": str(e)
            })

    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }


def start_textract_job(textract, s3_bucket, s3_key):
    response = textract.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket,
                'Name': s3_key
            }
        }
    )
    return response['JobId']


def poll_textract(textract, job_id):
    pages = {}
    next_token = None

    while True:
        if next_token:
            response = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
        else:
            response = textract.get_document_text_detection(JobId=job_id)

        print(f"Textract response: {response}")  # Log the response

        if response['JobStatus'] == 'SUCCEEDED':

            for block in response.get('Blocks', []):
                if block['BlockType'] == 'LINE':
                    page_number = block['Page']
                    if page_number not in pages:
                        pages[page_number] = []
                    pages[page_number].append(block['Text'])

            next_token = response.get('NextToken')
            if not next_token:
                break
        elif response['JobStatus'] == 'FAILED':
            raise Exception("Textract job failed.")

        time.sleep(5)

    return pages


def format_pages_with_numbers(pages):
    formatted_text = []
    for page_number, lines in pages.items():
        formatted_text.append(f"PDF Page Number {page_number}\n" + "\n".join(lines))
    return "\n\n".join(formatted_text)


def write_to_s3(bucket, key, data, content_type='text/plain'):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
