import boto3 #type:ignore
from time import sleep

def start_textract_job(s3_bucket, s3_key):
    textract = boto3.client('textract', region_name='us-east-1')
    response = textract.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket,
                'Name': s3_key
            }
        }
    )
    return response['JobId']

def poll_textract(job_id):
    textract = boto3.client('textract', region_name='us-east-1')
    status = None
    next_token = None
    pages = []

    while True:
        response_params = {'JobId': job_id}
        if next_token:
            response_params['NextToken'] = next_token

        response = textract.get_document_text_detection(**response_params)
        status = response['JobStatus']
        if status in ['SUCCEEDED', 'PARTIAL_SUCCESS']:
            current_page_text = []
            for block in response.get('Blocks', []):
                if block['BlockType'] == 'PAGE':
                    if current_page_text:
                        pages.append('\n'.join(current_page_text))
                        current_page_text = []
                elif block['BlockType'] == 'LINE':
                    current_page_text.append(block['Text'])
            if current_page_text:
                pages.append('\n'.join(current_page_text))
            next_token = response.get('NextToken', None)
            if not next_token:
                break
        elif status == 'IN_PROGRESS':
            sleep(5)
        else:
            raise Exception(f"Textract job failed with status: {status}")

    return pages



