import re

def remove_page_numbers(text, pattern=r'Page \d+ of \d+'):
    """
    Removes page numbers from text using a regular expression pattern.
    """
    page_number_pattern = re.compile(pattern, re.IGNORECASE)
    return page_number_pattern.sub('', text)

def add_page_numbers(pages):
    """
    Adds 'Page X:' before each page's text.
    """
    return [f"Page {i+1}\n{text}" for i, text in enumerate(pages)]
