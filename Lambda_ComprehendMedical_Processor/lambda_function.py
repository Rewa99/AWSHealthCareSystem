import boto3  # type:ignore
from urllib.parse import unquote_plus
import os
import re  # Import the regex module


# Ensure that your process_chunks.py has the necessary functions imported
from process_chunks import process_medical_chunks, process_pii_chunks

def replace_page_numbers_with_xylophone(text):
    # This regex will match "PDF Page Number X" where X is a digit
    return re.sub(r'PDF Page Number (\d+)', r'Xylophone', text)

def replace_xylophone_with_page_numbers(text, page_numbers):
    # Replace "Xylophone" back to the original PDF Page Numbers incrementally
    for i, number in enumerate(page_numbers, start=1):
        text = text.replace('Xylophone', f'PDF Page Number {number}', 1)  # Replace one at a time
    return text

def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    results = []
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        try:
            file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            extracted_text = file_obj['Body'].read().decode('utf-8')

            # Step 1: Replace PDF Page Numbers with "Xylophone"
            original_page_numbers = re.findall(r'PDF Page Number (\d+)', extracted_text)
            modified_text = replace_page_numbers_with_xylophone(extracted_text)

            file_base_name = os.path.splitext(os.path.basename(key))[0]
            chunks_folder = f"chunks/chunks_{file_base_name}/"
            pre_folder = chunks_folder + "pre/"
            post_folder = chunks_folder + "post/"

            # Process the text
            processed_text = ""
            processed_text += process_medical_chunks(bucket, pre_folder, post_folder, modified_text)
            processed_text = process_pii_chunks(bucket, pre_folder, post_folder, processed_text)

            # Step 3: Change "Xylophone" back to PDF Page Numbers incrementally
            processed_text = replace_xylophone_with_page_numbers(processed_text, original_page_numbers)

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
