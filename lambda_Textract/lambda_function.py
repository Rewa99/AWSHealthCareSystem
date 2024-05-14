import os
import json
from urllib.parse import unquote_plus

from text_processing import remove_page_numbers, add_page_numbers
from textract_utils import start_textract_job, poll_textract
from s3_utils import write_to_s3

def lambda_handler(event, context):
    results = []
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        
        # Extract the base file name without its extension
        file_base_name = os.path.splitext(os.path.basename(key))[0]

        try:
            job_id = start_textract_job(bucket, key)
            pages = poll_textract(job_id)
            cleaned_pages = [remove_page_numbers(page) for page in pages]
            processed_pages = add_page_numbers(cleaned_pages)
            
            # Construct the result_key to include the 'cleaned_input' subfolder
            result_key = f"output/cleaned_input/cleaned_{file_base_name}.txt"
            
            write_to_s3(bucket, result_key, '\n'.join(processed_pages))
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
