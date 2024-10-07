
import boto3  
from replace_utilities import replace_birthdate, replace_date, replace_pii, replace_gender

def process_medical_chunks(bucket, pre_folder, post_folder, text):
    comprehend_medical_client = boto3.client('comprehendmedical', region_name='us-east-1')
    s3_client = boto3.client('s3')

    max_size = 18000  # Comprehend Medical size limit
    chunks = [text[i:i + max_size] for i in range(0, len(text), max_size)]
    processed_text = ""

    for i, chunk in enumerate(chunks):
        pre_medical_chunk_key = f"{pre_folder}medical_chunk_{i+1}.txt"
        s3_client.put_object(Bucket=bucket, Key=pre_medical_chunk_key, Body=chunk)
        
        chunk = chunk[:20000]
        
        chunk = replace_birthdate(chunk)

        # Detect PHI entities
        phi_response = comprehend_medical_client.detect_phi(Text=chunk)
        for entity in phi_response['Entities']:
            
            # Skip insurance and any entity type that's not in our allowed list
            if entity['Type'] == 'INSURANCE':
                continue  # Leave insurance names unmodified
            
            allowed_types = ['DATE', 'DATE_TIME', 'AGE', 'TIME', 'NAME', 'PROFESSION', 'GENDER']
            if entity['Type'] not in allowed_types:
                continue  # Skip entities not in the allowed list

            # Handle entity types with placeholders
            placeholder = '[PHI]'  # Default placeholder if no specific type is matched
            if entity['Type'] == 'DATE' or entity['Type'] == 'DATE_TIME':
                placeholder = replace_date(entity['Text'])  # Use date placeholder logic
            elif entity['Type'] == 'AGE':
                placeholder = '[AGE]'
            elif entity['Type'] == 'TIME':
                placeholder = '[TIME]'
            elif entity['Type'] in ['NAME', 'PROFESSION']:
                placeholder = '[NAME]'
            elif entity['Type'] == 'GENDER':
                placeholder = '[GENDER]'

            # Replace detected PHI with the placeholder
            chunk = replace_pii(chunk, entity['Text'], placeholder)

        chunk = replace_gender(chunk)

        # Upload processed chunk to S3
        post_medical_chunk_key = f"{post_folder}medical_chunk_{i+1}.txt"
        s3_client.put_object(Bucket=bucket, Key=post_medical_chunk_key, Body=chunk)
        processed_text += chunk

    return processed_text


def process_pii_chunks(bucket, pre_folder, post_folder, text):
    comprehend_client = boto3.client('comprehend', region_name='us-east-2')
    s3_client = boto3.client('s3')

    max_size = 4500  
    chunks = [text[i:i + max_size] for i in range(0, len(text), max_size)]
    processed_text = ""

    for i, chunk in enumerate(chunks):
        pre_pii_chunk_key = f"{pre_folder}pii_chunk_{i+1}.txt"
        s3_client.put_object(Bucket=bucket, Key=pre_pii_chunk_key, Body=chunk)
        
        chunk = chunk[:5000]
        
        pii_response = comprehend_client.detect_pii_entities(Text=chunk, LanguageCode='en')
        for entity in pii_response['Entities']:
            placeholder = None
            if entity['Type'] == 'SSN':
                placeholder = '[SSN]'
            elif entity['Type'] == 'INTERNATIONAL_BANK_ACCOUNT_NUMBER':
                placeholder = '[IBAN]'
            elif entity['Type'] == 'SWIFT_CODE':
                placeholder = '[SWIFT]'
            elif entity['Type'] == 'BANK_ROUTING':
                placeholder = '[BANK_ROUTING]'
            elif entity['Type'] == 'PASSPORT_NUMBER':
                placeholder = '[PASSPORT]'
            elif entity['Type'] == 'LICENSE_PLATE':
                placeholder = '[LICENSE_PLATE]'
            elif entity['Type'] == 'VEHICLE_IDENTIFICATION_NUMBER':
                placeholder = '[VIN]'
            elif entity['Type'] == 'CREDIT_DEBIT_NUMBER':
                placeholder = '[CREDIT_DEBIT]'
            elif entity['Type'] == 'DRIVER_ID':
                placeholder = '[DRIVER_ID]'
            elif entity['Type'] == 'URL':
                placeholder = '[URL]'

            if placeholder:
                chunk = replace_pii(chunk, chunk[entity['BeginOffset']:entity['EndOffset']], placeholder)

        post_pii_chunk_key = f"{post_folder}pii_chunk_{i+1}.txt"
        s3_client.put_object(Bucket=bucket, Key=post_pii_chunk_key, Body=chunk)
        processed_text += chunk

    return processed_text
