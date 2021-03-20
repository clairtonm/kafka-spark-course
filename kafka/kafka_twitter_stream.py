import tweepy
import sys

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.id_str)
        # if "retweeted_status" attribute exists, flag this tweet as a retweet.
        is_retweet = hasattr(status, "retweeted_status")

        # check if text has been truncated
        if hasattr(status,"extended_tweet"):
            text = status.extended_tweet["full_text"]
        else:
            text = status.text

        # check if this is a quote tweet.
        is_quote = hasattr(status, "quoted_status")
        quoted_text = ""
        if is_quote:
            # check if quoted tweet's text has been truncated before recording it
            if hasattr(status.quoted_status,"extended_tweet"):
                quoted_text = status.quoted_status.extended_tweet["full_text"]
            else:
                quoted_text = status.quoted_status.text

        print(status.created_at, status.favorite_count, status.retweet_count, 
             status.text, status.id, status.geo, status.lang, status.source, status.entities.get('hashtags'))
        print('------------------------------------------')

    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")
        sys.exit()

ACCESS_TOKEN = '378933214-lF13qrhZtbXAXffoEF9WoDlp2JA709UqmXepHqSn'
ACCESS_TOKEN_SECRET = 'GPSVwNxol2YI1Oml1S7NGEkmD13zep4AGNObdhrBfw8gV'
API_KEY = 'gulk3K6vdFUw0XumlPhrUoPWI'
API_TOKEN = 'Go2chQVwPn2si2rB99FjqJy7dloC01cYtwbWcGzxgN5UBA2xFT'

auth = tweepy.OAuthHandler(API_KEY, API_TOKEN)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

streamListener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=streamListener, tweet_mode='extended')
stream.filter(track=['covid'])
