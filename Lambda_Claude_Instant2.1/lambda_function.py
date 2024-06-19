import json
import boto3
import csv
import time
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime')

def lambda_handler(event, context):
    try:
        start_time = time.time()
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        if not object_key.startswith('anonymized_output/'):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid S3 event.'})
            }

        anonymized_text = get_s3_object(bucket_name, object_key)
        questions = get_questions_from_csv(bucket_name, 'questions/questions.csv')

        for run in range(1, 4):  # Runs the document thrice
            for temperature in [0, 0.5, 1]:  # Runs with temperatures 0, 0.5 and 1
                with ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_question = {executor.submit(ask_bedrock, anonymized_text, question, temperature): question for question in questions}
                    answers = [(question, future.result()) for future, question in future_to_question.items()]

                    # Sort the answers based on the order of questions
                    answers.sort(key=lambda x: questions.index(x[0]))

                    # Extract only answers from sorted list
                    sorted_answers = [answer[1] for answer in answers]

                    answers_csv = generate_csv(sorted_answers)
                    answers_object_key = f"answers/{object_key.split('/')[-1].replace('.txt', '')}_run{run}_temperature{temperature}_answers.csv"
                    upload_csv_to_s3(bucket_name, answers_object_key, answers_csv)

        duration = time.time() - start_time
        print(f"Lambda function completed in {duration:.2f} seconds")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Answers saved successfully.'})
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

def generate_csv(answers):
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    for answer in answers:
        writer.writerow([answer])
    csv_buffer.seek(0)
    return csv_buffer.getvalue()

def upload_csv_to_s3(bucket_name, object_key, data):
    try:
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=data.encode('utf-8'), ContentType='text/csv')
    except Exception as e:
        print(f"Error uploading CSV to S3: {e}")
        raise
