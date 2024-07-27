import json
from s3_utils import get_s3_object, read_questions, read_keywords, upload_csv_to_s3
from text_utils import preprocess_text, generate_csv
from bedrock_utils import ask_bedrock

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        print(f"Bucket Name: {bucket_name}")
        print(f"Object Key: {object_key}")

        if not object_key.startswith('anonymized_output/'):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid S3 event.'})
            }

        anonymized_text = get_s3_object(bucket_name, object_key)
        questions = read_questions(bucket_name, 'questions/question.csv')
        keywords_df = read_keywords(bucket_name, 'keywords/keywords2.csv')

        answers = []

        for _, row in questions.iterrows():
            question = row[0]  # Assuming the question is the first column
            sr_no = row[1]    # Assuming Sr.No is the second column
            keywords = keywords_df[keywords_df['Sr.No'] == sr_no]['Keywords'].tolist()  # Access keywords by index
            relevant_text = preprocess_text(anonymized_text, keywords)
            answer = ask_bedrock(relevant_text, question)
            answers.append((question, answer))

        answers_csv = generate_csv(answers)
        answers_object_key = f"answers/{object_key.split('/')[-1].replace('.txt', '')}_answers.csv"
        upload_csv_to_s3(bucket_name, answers_object_key, answers_csv)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Answers saved successfully.', 'total_api_hits': len(answers)})
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
