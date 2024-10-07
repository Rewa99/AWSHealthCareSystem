import pandas as pd
import openai
import time
import math

# Define file paths
csv_file_path = '/Users/rewadeshpande/Downloads/questionsnew.xlsx'  # Adjust if necessary
text_file_path = '/Users/rewadeshpande/CodeProjects/AWSHealthCareSystem/S3_Bucket_Contents/New Textract Outputs/ComprehendMedical/anonymized_textract_Case_study_3_ZB_Redacted.txt'
output_csv_path = '/Users/rewadeshpande/PycharmProjects/Test1/newoutputs.csv'

openai.api_key = '' #OpenAPI Key

def get_text_from_file(text_file_path):
    """Reads the entire text file."""
    with open(text_file_path, 'r') as file:
        return file.read()

def get_gpt_response(prompt, retries=3):
    """Get response from GPT model with retry logic."""
    for attempt in range(retries):
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                top_p=1.0,
                max_tokens=1500
            )
            return response.choices[0].message['content'].strip()
        except openai.error.RateLimitError:
            wait_time = 10  # Increase wait time if needed
            print(f"Rate limit error encountered. Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"Error: {e}")
            return None

def process_batch(batch, context):
    """Process a batch of requests."""
    batch_results = []
    for index, row in batch.iterrows():
        question = row['Questions']
        prompt = f"Context: {context}\nQuestion: {question}"
        answer = get_gpt_response(prompt)
        batch_results.append(answer)
        print(f"Processed question {index + 1}: {question}\nAnswer: {answer}")
    return batch_results

def main():
    context = get_text_from_file(text_file_path)

    # Try reading the file as an Excel file
    try:
        df = pd.read_excel(csv_file_path)  # Adjust if the file is actually a CSV
    except Exception as e:
        print(f"Error reading the Excel file: {e}")
        return

    df['Answers'] = ""

    batch_size = 5
    total_rows = len(df)
    num_batches = math.ceil(total_rows / batch_size)

    for batch_number in range(num_batches):
        print(f"Processing batch {batch_number + 1} of {num_batches}")

        start_index = batch_number * batch_size
        end_index = min(start_index + batch_size, total_rows)

        batch = df.iloc[start_index:end_index]
        batch_results = process_batch(batch, context)

        df.loc[start_index:end_index - 1, 'Answers'] = batch_results

        time.sleep(5)

    df.to_csv(output_csv_path, index=False)
    print(f"Answers saved to {output_csv_path}")

if __name__ == "__main__":
    main()
