import json
import boto3
import csv
import concurrent.futures
from io import StringIO
from s3_operations import get_s3_object, upload_csv_to_s3
from bedrock_operations import ask_bedrock

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        if not object_key.startswith('anonymized_output/'):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid S3 event.'})
            }

        anonymized_text = get_s3_object(bucket_name, object_key)
        questions = get_questions_from_csv(bucket_name, 'questions/questions.csv')

        for run in range(1, 4):
            answers = execute_runs(anonymized_text, questions)

            # Generate CSV and upload to S3
            answers_csv = generate_csv(answers)
            answers_object_key = f"answers/{object_key.split('/')[-1].replace('.txt', '')}_answers_run{run}.csv"
            upload_csv_to_s3(bucket_name, answers_object_key, answers_csv)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Answers saved successfully for all runs.'})
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_questions_from_csv(bucket_name, object_key):
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    content = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.reader(content)
    questions = [row[0] for row in reader if row]
    return questions

def execute_runs(anonymized_text, questions):
    answers = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_question = {executor.submit(ask_bedrock, anonymized_text, question): i for i, question in enumerate(questions)}

        for future in concurrent.futures.as_completed(future_to_question):
            index = future_to_question[future]
            try:
                answer = future.result()
                answers.append((questions[index], answer))  # Append both question and answer as tuple
            except Exception as exc:
                print(f'Question {questions[index]} generated an exception: {exc}')

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
