import json
import boto3
import csv
import time
from io import StringIO
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime')

# Constants
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

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

        # Get questions from the specified S3 bucket and key
        questions_bucket = 'actualwise-bucket-store'
        questions_key = 'questions/questions.csv'
        questions_csv = get_s3_object(questions_bucket, questions_key)
        questions = read_questions_from_csv(questions_csv)

        # Process questions in batches
        batch_size = 5  # Adjust batch size as needed
        answers = []
        for i in range(0, len(questions), batch_size):
            batch_questions = questions[i:i + batch_size]
            batch_answers = process_questions_batch(anonymized_text, batch_questions)
            answers.extend(batch_answers)

        # Prepare answer for CSV generation
        answers_csv = generate_csv(answers)
        answers_object_key = f"answers/{object_key.split('/')[-1].replace('.txt', '')}_answers.csv"
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

def read_questions_from_csv(csv_data):
    questions = []
    csv_reader = csv.reader(StringIO(csv_data))
    for row in csv_reader:
        if row:
            questions.append(row[0])  # Assuming questions are in the first column
    return questions

def process_questions_batch(anonymized_text, questions):
    answers = []
    for question in questions:
        answer = ask_bedrock(anonymized_text, question)
        answers.append(answer)
    return answers

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
    