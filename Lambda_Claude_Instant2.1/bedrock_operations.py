import json
import boto3
import time
from botocore.exceptions import ClientError

bedrock = boto3.client('bedrock-runtime')

def ask_bedrock(text, question, temperature):
    model_id = 'anthropic.claude-instant-v1'
    payload = {
        "prompt": f"\n\nHuman: {text}\n\nQuestion: {question}\nAssistant:",
        "max_tokens_to_sample": 300,
        "temperature": temperature,
        "top_k": 250,
        "top_p": 1,
        "stop_sequences": ["\n\nHuman:"],
        "anthropic_version": "bedrock-2023-05-31"
    }

    retries = 3
    for attempt in range(retries):
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

                if 'completion' in response_json:
                    return response_json['completion'].strip()
                else:
                    raise Exception(f"Failed to get response from Bedrock: {response_json}")
            else:
                raise Exception(f"'body' key not found in the response: {response}")
        except ClientError as e:
            print(f"ClientError invoking Bedrock model: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
        except Exception as e:
            print(f"Error invoking Bedrock model: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
