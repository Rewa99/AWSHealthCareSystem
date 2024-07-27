def preprocess_text(anonymized_text, keywords):
    relevant_pages = []
    pages = anonymized_text.split('PDF Page Number')

    for page in pages:
        if any(keyword in page for keyword in keywords):
            relevant_pages.append(page.strip())

    return ' '.join(relevant_pages)

def generate_csv(answers):
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(['Question', 'Answer'])
    for answer in answers:
        writer.writerow(answer)
    csv_buffer.seek(0)
    return csv_buffer.getvalue()
