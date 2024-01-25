import db
import logging, os
import boto3
logging.basicConfig(level=logging.INFO)

def main():
    sqs_endpoint = os.getenv('SQS_ENDPOINT')
    sqs_client = boto3.client('sqs')
    # TODO: Add interfaces here request queing for zip code look request, part of the main program.
