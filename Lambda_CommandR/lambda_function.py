import json
import time
import csv
from io import StringIO
from s3_operations import get_s3_object, upload_csv_to_s3, read_questions_from_csv
from bedrock_operations import ask_bedrock

# Constants
BATCH_SIZE = 5  # Adjust batch size as needed
QUESTIONS_BUCKET = 'actualwise-bucket-store'
QUESTIONS_KEY = 'questions/questions.csv'

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
        questions_csv = get_s3_object(QUESTIONS_BUCKET, QUESTIONS_KEY)
        questions = read_questions_from_csv(questions_csv)

        # Process questions in batches
        answers = []
        for i in range(0, len(questions), BATCH_SIZE):
            batch_questions = questions[i:i + BATCH_SIZE]
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

def process_questions_batch(anonymized_text, questions):
    answers = []
    for question in questions:
        answer = ask_bedrock(anonymized_text, question)
        answers.append(answer)
    return answers

def generate_csv(answers):
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    for answer in answers:
        writer.writerow([answer])
    csv_buffer.seek(0)
    return csv_buffer.getvalue()
