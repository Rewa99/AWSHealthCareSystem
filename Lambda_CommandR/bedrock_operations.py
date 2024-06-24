import json
import time
import boto3
from botocore.exceptions import ClientError

bedrock = boto3.client('bedrock-runtime')

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

def ask_bedrock(text, question):
    model_id = 'cohere.command-r-v1:0'
    payload = {
        "chat_history": [
            {"role": "USER", "message": text},
            {"role": "CHATBOT", "message": "Processing text..."}
        ],
        "message": question
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = bedrock.invoke_model(
                modelId=model_id,
                body=json.dumps(payload),
                contentType='application/json',
                accept='application/json'
            )

            if 'body' in response:
                response_text = response['body'].read().decode('utf-8')
                response_json = json.loads(response_text)

                print(f"Bedrock response: {response_json}")

                if 'text' in response_json:
                    return response_json['text'].strip()
                else:
                    raise Exception(f"Failed to get answer from Bedrock response: {response_json}")
            else:
                raise Exception(f"'body' key not found in the response: {response}")
        except ClientError as e:
            print(f"ClientError invoking Bedrock model: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SECONDS * 2 ** attempt)
            else:
                raise
        except Exception as e:
            print(f"Error invoking Bedrock model: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SECONDS * 2 ** attempt)
            else:
                raise
