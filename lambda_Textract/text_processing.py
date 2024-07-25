import re

def remove_page_numbers(text):
    """
    Removes page numbers from text using a regular expression pattern.
    """
    patterns = [
        r'Page \d+ of \d+',   # Page 1 of 10
        r'Page \d+/\d+',      # Page 1/10
        r'Page \d+',          # Page 1
        r'Page of \d+'        # Page of 10
    ]

    for pattern in patterns:
        page_number_pattern = re.compile(pattern, re.IGNORECASE)
        text = page_number_pattern.sub('', text)

    return text

def add_page_numbers(pages):
    """
    Adds 'PDF Page Number X:' before each page's text.
    """
    return [f"PDF Page Number {i+1}\n{text}" for i, text in enumerate(pages)]
