import pandas as pd
import openai
import time

openai.api_key = '' #OpenAPI Key

# Define file paths
csv_file_path = '/Users/rewadeshpande/PycharmProjects/Test1/Page Numbers/keywords_page_numbers_CaseStudy1.csv'
text_file_path = '/Users/rewadeshpande/PycharmProjects/Test1/Case Study/anonymized_cleaned_Case_study_1_CO_Redacted.txt'
output_csv_path = '/Users/rewadeshpande/PycharmProjects/Test1/Keywords and Questions.csv'

# Function to extract text from specific pages
def get_text_from_pages(text_file_path, page_numbers):
    """Extracts and returns text from specific pages."""
    with open(text_file_path, 'r') as file:
        content = file.read()

    # Split the document into pages based on "PDF Page Number"
    pages = content.split('PDF Page Number ')
    relevant_text = ""
    for page_number in page_numbers:
        for page in pages:
            if page.startswith(str(page_number)):
                relevant_text += page + "\n"
                break
    return relevant_text

# Function to get response from GPT model
def get_gpt_response(prompt):
    """Get response from GPT model."""
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
    except Exception as e:
        print(f"Error: {e}")
        return None

def process_questions(df, text_file_path):
    """Process questions without batching."""
    for index, row in df.iterrows():
        question = row['Questions']
        page_numbers = row['Page Numbers']

        if pd.isna(page_numbers) or not page_numbers.strip():
            # If no page numbers are mentioned, set the answer to "NO"
            answer = "NO"
        else:
            # Convert the page numbers string to a list of integers
            page_numbers_list = list(map(int, page_numbers.split(',')))
            context = get_text_from_pages(text_file_path, page_numbers_list)
            # Construct the prompt with context and the question
            prompt = f"Context: {context}\nQuestion: {question}\nIf no relevant information is found in the context or no pages are mentioned, respond with 'NO'."
            answer = get_gpt_response(prompt)

        # Update the DataFrame with the answer
        df.at[index, 'Answers'] = answer
        print(f"Processed question {index + 1} with page numbers {page_numbers}: {answer}")

        # Optional: Delay between requests to avoid rate limit issues
        time.sleep(3)

# Main function
def main():
    try:
        # Read the CSV file with a different encoding to avoid Unicode errors
        df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Add a new column for answers
    df['Answers'] = ""

    # Process the questions one by one
    process_questions(df, text_file_path)

    # Save the updated DataFrame with answers to the CSV file
    df.to_csv(output_csv_path, index=False)
    print(f"Answers saved to {output_csv_path}")

if __name__ == "__main__":
    main()
