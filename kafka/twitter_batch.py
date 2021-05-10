import tweepy
import configparser
from datetime import datetime
import json

config = configparser.ConfigParser()
config.read('./twitter.ini')

ACCESS_TOKEN = config.get('twitter', 'ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = config.get('twitter', 'ACCESS_TOKEN_SECRET')
API_KEY = config.get('twitter', 'API_KEY')
API_TOKEN = config.get('twitter', 'API_TOKEN')

auth = tweepy.OAuthHandler(API_KEY, API_TOKEN)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)

#procurar por covid, tweets que não foram replies ou retweets
search = "covid -filter:replies -filter:retweets"

results = tweepy.Cursor(api.search, q=search, since="2020-03-10",  result_type="mixed").items(1000)

tweets = []
count = 0
for result in results:
    tweet = {
        "created_at": datetime.timestamp(result.created_at),
        "hashtags": result.entities['hashtags'],
        "favorite_count": result.favorite_count,
        "retweet_count": result.retweet_count,
        "text": result.text,
        "id": result.id,
        "geo": result.geo,
        "lang": result.lang
    }
    count += 1
    tweets.append(tweet)

print(count)
with open('../data/tweets.json', 'w') as jsonFile:
    json.dump(tweets, jsonFile)



