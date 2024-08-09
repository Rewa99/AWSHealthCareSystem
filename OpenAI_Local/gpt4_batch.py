import pandas as pd #type:ignore
import re
import openai   #type:ignore
import time
import os

openai.api_key = os.getenv('OPENAI_API_KEY')

BATCH_SIZE = 5  

def read_text_file(file_path):
    with open(file_path, 'r') as file:
        text = file.read()
    pages = re.split(r'(PDF Page Number \d+)', text)
    pages = [(pages[i], pages[i + 1]) for i in range(1, len(pages) - 1, 2)]
    page_contents = {}
    for page_marker, page_content in pages:
        page_number = re.search(r'\d+', page_marker).group()
        page_contents[page_number] = page_content.lower()
    return page_contents

def get_relevant_text_from_pages(page_numbers, page_contents):
    relevant_text = ''
    for page_number in page_numbers.split(', '):
        if page_number in page_contents:
            relevant_text += page_contents[page_number] + ' '
    return relevant_text

def ask_gpt_with_retry(question, context, retries=3):
    for _ in range(retries):
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": f"Context: {context}\nQuestion: {question}"}
                ]
            )
            return response.choices[0]['message']['content'].strip()
        except openai.error.RateLimitError:
            print("Rate limit reached. Retrying in 5 seconds...")
            time.sleep(5)
    raise Exception("Failed to get a response from GPT after retries.")

def process_batches(keywords_df, page_contents, batch_size):
    total_questions = len(keywords_df)
    answers = []

    for i in range(0, total_questions, batch_size):
        batch_df = keywords_df.iloc[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1}/{(total_questions + batch_size - 1) // batch_size}...")

        batch_answers = []
        for _, row in batch_df.iterrows():
            question = row['Question']
            page_numbers = row['Page Numbers']
            context = get_relevant_text_from_pages(page_numbers, page_contents)
            answer = ask_gpt_with_retry(question, context)
            batch_answers.append(answer)

        answers.extend(batch_answers)
        print("Batch processed, pausing to avoid rate limits...")
        time.sleep(10)  

    return answers

def add_answers_to_csv(keywords_df, answers):
    keywords_df['Answers'] = answers
    output_file = 'questions_with_keywords_and_answers.csv'
    keywords_df.to_csv(output_file, index=False)
    print(f"Answers saved to {output_file}")

if __name__ == "__main__":
    keywords_csv_path = 'questions_with_keywords_and_pages.csv'  
    text_file_path = 'anonymized_cleaned_Case_study_1_CO_Redacted.txt'

    keywords_df = pd.read_csv(keywords_csv_path)

    page_contents = read_text_file(text_file_path)

    answers = process_batches(keywords_df, page_contents, BATCH_SIZE)

    add_answers_to_csv(keywords_df, answers)
