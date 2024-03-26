import boto3
import os
import json
import botocore
from difflib import SequenceMatcher

# Set up AWS services
s3 = boto3.client('s3')
comprehend = boto3.client('comprehend')
textract = boto3.client('textract')

# Set up similarity threshold
SIMILARITY_THRESHOLD = 0.0

import base64

def lambda_handler(event, context):
    # Get reference PDF file from S3
    reference_pdf_bucket = ''
    prefix=''
    reference_pdf_key = get_most_recent_object(reference_pdf_bucket,prefix)
    print('rpk',reference_pdf_key)
    reference_pdf = s3.get_object(Bucket=reference_pdf_bucket, Key=reference_pdf_key)
    pdf_bytes = reference_pdf['Body'].read()  # Read the raw PDF bytes
    base64_encoded_bytes = base64.b64encode(pdf_bytes)  # Convert to base64-encoded bytes

    print(type(base64_encoded_bytes))
    reference_pdf_text = extract_text_from_pdf(textract, base64_encoded_bytes)  # Pass textract_client and base64_encoded_bytes

    # Get folder of PDF files from S3
    pdf_folder_bucket = ''
    pdf_folder_key = ""
    pdf_files = get_pdf_files_in_folder(pdf_folder_bucket, pdf_folder_key)
    
    # Compare similarity of each PDF file with reference PDF
    similarity_scores = {}
    for pdf_file in pdf_files:
        pdf_file_text = extract_text_from_pdf(textract, pdf_file)
        similarity_score = calculate_similarity(reference_pdf_text, pdf_file_text)
        print(similarity_score*100)
        if similarity_score >= SIMILARITY_THRESHOLD:
            similarity_scores[pdf_file] = similarity_score
    
    # Sort similarity scores in descending order
    similarity_scores = dict(sorted(similarity_scores.items(), key=lambda item: item[1], reverse=True))
    
    # Create string response body
    body = ""
    for key, value in similarity_scores.items():
        key_str = base64.b64decode(key).decode('iso-8859-1')
        body += f"{key_str}: {value}\n"
    
    # Return response as string
    return {
        'statusCode': 200,
        'body': "body"
    }
    
def calculate_similarity(text1, text2):
    return SequenceMatcher(None, text1, text2).ratio()


def extract_text_from_pdf(textract_client, pdf_bytes):
    """Extracts text from a PDF document using Textract.

    Handles UnsupportedDocumentException gracefully.
    """
    try:
        response = textract_client.analyze_document(Document={'Bytes': pdf_bytes}, FeatureTypes=['TABLES', 'FORMS'])
        text = b''
        # Extract text from lines
        for block in response['Blocks']:
            if block['BlockType'] == 'LINE' and block['Text']:
                text += block['Text'].encode('utf-8') + b' '
        return text
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'UnsupportedDocumentException':
            print(f"Textract encountered an unsupported document format: {error}")
            return b''  # Empty bytes on unsupported format
        else:
            raise error  # Re-raise other exceptions


def get_pdf_files_in_folder(bucket, folder_key):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=folder_key)
    pdf_files = []
    for item in response['Contents']:
        if item['Key'].endswith('.pdf'):
            obj = s3.get_object(Bucket=bucket, Key=item['Key'])
            pdf_files.append(obj['Body'].read())
    return pdf_files


def get_most_recent_object(bucket, prefix):
    """Retrieve the most recent object from an S3 bucket based on the prefix."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    # Sort objects by LastModified time and get the most recent one
    if 'Contents' in response:
        most_recent_object = max(response['Contents'], key=lambda x: x['LastModified'])
        return most_recent_object['Key']
    else:
        raise Exception("No objects found in the specified bucket and prefix.")
