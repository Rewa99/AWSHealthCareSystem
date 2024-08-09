import boto3 #type:ignore
from urllib.parse import unquote_plus
import os
from process_chunks import process_medical_chunks, process_pii_chunks

def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    results = []
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        try:
            file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            extracted_text = file_obj['Body'].read().decode('utf-8')

            file_base_name = os.path.splitext(os.path.basename(key))[0]
            chunks_folder = f"chunks/chunks_{file_base_name}/"
            pre_folder = chunks_folder + "pre/"
            post_folder = chunks_folder + "post/"

            processed_text = ""

            # Process medical chunks
            processed_text += process_medical_chunks(bucket, pre_folder, post_folder, extracted_text)

            # Process PII chunks
            processed_text = process_pii_chunks(bucket, pre_folder, post_folder, processed_text)

            # Save the final anonymized file
            anonymized_key = f"anonymized_output/anonymized_{file_base_name}.txt"
            s3_client.put_object(Bucket=bucket, Key=anonymized_key, Body=processed_text)
            results.append(f"Processed and stored {anonymized_key}")

        except Exception as e:
            results.append(f"Failed to process {key}: {str(e)}")

    return {
        'statusCode': 200,
        'body': '\n'.join(results)
    }


