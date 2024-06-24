import json
import boto3
import csv
import time
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
from s3_operations import get_s3_object, upload_csv_to_s3, get_questions_from_csv
from bedrock_operations import ask_bedrock

s3 = boto3.client('s3')

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
                answers = execute_with_temperature(anonymized_text, questions, temperature)

                # Generate CSV and upload to S3
                answers_csv = generate_csv(answers)
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

def execute_with_temperature(anonymized_text, questions, temperature):
    answers = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_question = {executor.submit(ask_bedrock, anonymized_text, question, temperature): question for question in questions}
        answers = [(question, future.result()) for future, question in future_to_question.items()]

        # Sort the answers based on the order of questions
        answers.sort(key=lambda x: questions.index(x[0]))

        # Extract only answers from sorted list
        sorted_answers = [answer[1] for answer in answers]

    return sorted_answers

def generate_csv(answers):
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    for answer in answers:
        writer.writerow([answer])
    csv_buffer.seek(0)
    return csv_buffer.getvalue()
