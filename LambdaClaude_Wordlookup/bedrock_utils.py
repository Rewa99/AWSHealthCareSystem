import boto3
import json

bedrock = boto3.client('bedrock-runtime')

def ask_bedrock(text, question):
    model_id = 'anthropic.claude-3-haiku-20240307-v1:0'
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1000,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"{text}\n\nQuestion: {question}\nAnswer:"
                    }
                ]
            }
        ]
    }

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

            if 'content' in response_json:
                return response_json['content'][0]['text'].strip()
            else:
                raise Exception(f"Failed to get response from Bedrock: {response_json}")
        else:
            raise Exception(f"'body' key not found in the response: {response}")
    except Exception as e:
        print(f"Error invoking Bedrock model: {e}")
        raise
