import imp
import sys
sys.modules["sqlite"] = imp.new_module("sqlite")
sys.modules["sqlite3.dbapi2"] = imp.new_module("sqlite.dbapi2")

from chalice import Chalice, Rate
from finviz.main_func import get_news
from newspaper import Article
import boto3
import json
from datetime import datetime
import nltk
nltk.data.path.append("/tmp")
nltk.download('punkt', download_dir = "/tmp")

# Create app instance
app = Chalice(app_name="Lambda1")

"""Gather and extract data every 3 days"""
@app.schedule(Rate(72, unit=Rate.HOURS))
def periodic_task(event):

    """Create dict with url, headline, and article text
    """

    stocks = ['SPY' 'CRWD' 'LYFT' 'UBER' 'BYND' 'WORK' 'ZM']
    headlines = []
    urls = []
    tickers = []

    for stock in stocks:
      news = get_news(stock)
      headlines += [i[0] for i in news]
      urls += [i[1] for i in news]
      tickers += [stock for i in range(len(news))]

    text = []
    publish_dates = []
    keywords = []
    summaries = []

    for url in urls:
        try:
            article = Article(url)
            article.download()
            article.parse()
            article.nlp()
            text.append(article.text)
            publish_dates.append(article.publish_date)
            keywords.append(article.keywords)
            summaries.append(article.summary)
        except:
            text.append(None)
            publish_dates.append(None)
            keywords.append(None)
            summaries.append(None)
            continue

    data = ({'timestamp': str(datetime.now()),
             'publish_date': [str(i) for i in publish_dates],
             'ticker': tickers,
             'url': urls,
             'headline': headlines,
             'full text': text,
             'summary': summaries,
             'keywords': keywords})

    client = boto3.client('s3')
    client.put_object(Bucket='<your_bucket_name>', Body=json.dumps(data),
                      Key='<your_key_name>')
    # Send a message
    sns = boto3.client('sns')
    topic_arn = [t['TopicArn'] for t in sns.list_topics()['Topics']
                 if t['TopicArn'].endswith(':<your_topic_name>')][0]
    sns.publish(Message='Lambda1 has finished execution',
                Subject='Lambda1Event',TopicArn=topic_arn)

    return 'Data gathered and stored to bucket'
