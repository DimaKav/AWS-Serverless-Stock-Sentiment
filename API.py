from chalice import Chalice
import boto3
import json
import pandas as pd
import numpy as np

app = Chalice(app_name="Lambda4")

"""API for serving aggregated data"""
@app.route("/")
def index():

    resource = boto3.resource('s3')
    bucket = resource.Bucket('finviznews')
    client = boto3.client('s3')
    # Grab all the objects in the folder
    paths = []
    for object_summary in bucket.objects.filter(Prefix="transformed/"):
        paths.append(str(object_summary.key))
    paths = [i for i in paths if i.endswith('.json')]
    # Put them into 1 dataframe (temporary until data gets big)
    df = pd.DataFrame()
    for path in paths:
        object = client.get_object(Bucket='finviznews', Key=path)
        data = object['Body'].read()
        df = df.append(pd.read_json(data))

    df = df.reset_index(drop=True)
    # make 'None' nan
    df['publish_date'] = df.publish_date.replace('None', np.nan)
    # Fill nas with closest valid observatiokn
    df['publish_date'] = df.publish_date.fillna(method='bfill')

    return df.to_json()