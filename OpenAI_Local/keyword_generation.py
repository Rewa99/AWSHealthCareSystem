import pandas as pd                         #type: ignore
import nltk                                 #type: ignore
from nltk.tokenize import word_tokenize     #type: ignore
from nltk.corpus import stopwords           #type: ignore
from collections import Counter             #type: ignore

excluded_words = set(['drugs', 'page', 'patient'])


def extract_keywords(text, num_keywords=10):
    tokens = word_tokenize(text.lower())
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [token for token in tokens if
                       token.isalpha() and token not in stop_words and token not in excluded_words]
    freq_dist = Counter(filtered_tokens)
    most_common_keywords = [keyword for keyword, _ in freq_dist.most_common(num_keywords)]
    return ', '.join(most_common_keywords)


def process_csv(file_path):
    df = pd.read_csv(file_path, header=None)
    df['Keywords'] = df[0].apply(lambda text: extract_keywords(text, num_keywords=10))
    output_file = 'questions_with_keywords.csv'
    df.to_csv(output_file, index=False, header=['Question', 'Keywords'])
    print(f"Keywords extracted and saved to {output_file}")


if __name__ == "__main__":
    file_path = 'questions.csv' 
    process_csv(file_path)
