from chalice import Chalice
import boto3
import json
import pandas as pd
from datetime import datetime

app = Chalice(app_name="Lambda3")

"""On SNS message from Lambda2, aggregate sentiment data and store it"""
@app.on_sns_message(topic='Lambda2Event')
def handle_sns_message(event):

    client = boto3.client('s3')

    object = client.get_object(Bucket='finviznews',
                               Key='sentiment/ALLSTOCKS_withsentiment.json')
    data = object['Body'].read()
    df = pd.read_json(data)
    df = df.reset_index(drop=True)

    client.put_object(Bucket='finviznews', Body=df.to_json(),
                      Key=f'transformed/{datetime.now()}_aggregation.json')
