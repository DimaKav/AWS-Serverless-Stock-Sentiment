from chalice import Chalice
import boto3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from datetime import datetime

app = Chalice(app_name='Lambda2')

"""On SNS message from Lambda1, tranform data by dropping duplicates
and get sentiment"""
@app.on_sns_message(topic='Lambda1Event')
def handle_sns_message(event):

    client = boto3.client('s3')

    ANALYZER = SentimentIntensityAnalyzer()

    object = client.get_object(Bucket='<your_bucket_name>',
                               Key='<your_key_name')
    data = object['Body'].read()
    df = pd.read_json(data)
    # Drop headline duplicates
    df = df.drop_duplicates(subset=['headline'])
    # Drop all 'None' duplicates
    df = df.drop_duplicates(subset=['full text'],keep=False)
    df['sentiment'] = df['full text'].apply(ANALYZER.polarity_scores)
    df = df.drop('full text',axis=1)
    client.put_object(Bucket='<your_bucket_name>', Body=df.to_json(),
                      Key='<your_key_name>')
    # Send a message
    sns = boto3.client('sns')
    topic_arn = [t['TopicArn'] for t in sns.list_topics()['Topics']
                 if t['TopicArn'].endswith(':Lambda2Event')][0]
    sns.publish(Message='Lambda2 has finished execution',
                Subject='Lambda2Event',TopicArn=topic_arn)
