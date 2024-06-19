import json
import boto3
import csv
import concurrent.futures
from io import StringIO


s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime')


def lambda_handler(event, context):
    try:
        # Retrieve the S3 bucket and object key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']


        if not object_key.startswith('anonymized_output/'):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid S3 event.'})
            }


        anonymized_text = get_s3_object(bucket_name, object_key)


        questions = get_questions_from_csv(bucket_name, 'questions/questions.csv')


        batch_size = 5
        answers = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for i in range(0, len(questions), batch_size):
                batch_questions = questions[i:i + batch_size]
                futures = [executor.submit(ask_bedrock, anonymized_text, question) for question in batch_questions]
                for future in concurrent.futures.as_completed(futures):
                    question, answer = future.result()
                    answers.append({'question': question, 'answer': answer})

        
        answers_csv = generate_csv(answers)
        answers_object_key = f"answers/{object_key.split('/')[-1].replace('.txt', '')}_answers.csv"
        upload_csv_to_s3(bucket_name, answers_object_key, answers_csv)

        return {
            'statusCode': 200,
            'body': json.dumps(answers)
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def get_s3_object(bucket_name, object_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        body_bytes = response['Body'].read()
        body_string = body_bytes.decode('utf-8')
        return body_string
    except Exception as e:
        print(f"Error getting S3 object: {e}")
        raise


def get_questions_from_csv(bucket_name, object_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.reader(content)
        questions = [row[0] for row in reader if row]
        return questions
    except Exception as e:
        print(f"Error reading CSV: {e}")
        raise


def ask_bedrock(text, question):
    model_id = 'anthropic.claude-3-haiku-20240307-v1:0'  # Replace with your Bedrock model ID

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1000,
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
                return question, response_json['content'][0]['text'].strip()
            else:
                raise Exception(f"Failed to get response from Bedrock: {response_json}")
        else:
            raise Exception(f"'body' key not found in the response: {response}")
    except Exception as e:
        print(f"Error invoking Bedrock model: {e}")
        raise


def generate_csv(data):
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=['question', 'answer'])
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    csv_buffer.seek(0)
    return csv_buffer.getvalue()


def upload_csv_to_s3(bucket_name, object_key, data):
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=data.encode('utf-8'), ContentType='text/csv')

