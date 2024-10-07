import pandas as pd
import openai
import time

openai.api_key = '' #OpenAPI Key

# File paths
csv_file_path = '/Users/rewadeshpande/Downloads/questionsnew.xlsx'  # Adjust if necessary
text_file_path = '/Users/rewadeshpande/CodeProjects/AWSHealthCareSystem/S3_Bucket_Contents/New Textract Outputs/ComprehendMedical/anonymized_textract_Case_study_3_ZB_Redacted.txt'
output_csv_path = '/Users/rewadeshpande/PycharmProjects/Test1/Keywords and Questions.csv'


# Function to read the entire text document
def get_text_from_file(text_file_path):
    with open(text_file_path, 'r') as file:
        content = file.read()
    return content


# Function to get GPT response
def get_gpt_response(prompt, retry_count=5):
    for i in range(retry_count):
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                max_tokens=150
            )
            return response.choices[0].message['content'].strip()
        except openai.error.RateLimitError as e:
            # Handle rate limit error by sleeping and retrying
            print(f"Rate limit error: {e}. Retrying in {2 ** i} seconds.")
            time.sleep(2 ** i)  # Exponential backoff strategy
        except Exception as e:
            print(f"Error: {e}")
            return None
    return None  # Return None if retries fail


def main():
    # Read the questions from the CSV file with proper encoding and handle bad lines
    try:
        df = pd.read_csv(csv_file_path, encoding='ISO-8859-1', error_bad_lines=False,
                         engine='python')  # Handle bad lines
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    df['Answers'] = ""

    # Read the full text content from the file
    context = get_text_from_file(text_file_path)

    # Process questions one by one
    for index, row in df.iterrows():
        question = row['Questions']

        # Prepare the prompt with the full context and question
        prompt = f"Context: {context}\nQuestion: {question}"

        # Get the answer from GPT
        answer = get_gpt_response(prompt)

        # Save the answer in the DataFrame
        df.at[index, 'Answers'] = answer

        # Log progress
        print(f"Processed question {index + 1}: {answer}")

        # Add a small delay between requests to avoid rate limiting
        time.sleep(3)

    # Save the updated DataFrame to CSV
    df.to_csv(output_csv_path, index=False)
    print(f"Answers saved to {output_csv_path}")


if __name__ == "__main__":
    main()
