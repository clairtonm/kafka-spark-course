#from kafka import KafkaProducer
import twitter
import configparser
import os

ACCESS_TOKEN = '378933214-lF13qrhZtbXAXffoEF9WoDlp2JA709UqmXepHqSn'
ACCESS_TOKEN_SECRET = 'GPSVwNxol2YI1Oml1S7NGEkmD13zep4AGNObdhrBfw8gV'
API_KEY = 'gulk3K6vdFUw0XumlPhrUoPWI'
API_TOKEN = 'Go2chQVwPn2si2rB99FjqJy7dloC01cYtwbWcGzxgN5UBA2xFT'

# config = configparser.RawConfigParser()

# thisfolder = os.path.dirname(os.path.abspath(__file__))
# initfile = os.path.join(thisfolder, 'twitter.init')
# config.read(initfile)
# print(config.sections())

# print(config.get('twitter', 'API_KEY'))

api = twitter.Api(consumer_key=API_KEY,
                  consumer_secret=API_TOKEN,
                  access_token_key=ACCESS_TOKEN,
                  access_token_secret=ACCESS_TOKEN_SECRET)

# api = twitter.Api(consumer_key=[config.get("twitter", "API_KEY")],
#                   consumer_secret=[config.get("twitter","API_TOKEN")],
#                   access_token_key=[config.get("twitter","ACCESS_TOKEN")],
#                   access_token_secret=[config.get("twitter","ACCESS_TOKEN_SECRET")])

query = "q=(#covid OR #covid19) has:lang has:geo -is:retweet result_type=recent since=2021-03-01"
enconded_query = r"q%3D%28%23covid%20OR%20%23covid19%29%20has%3Alang%20has%3Ageo%20-is%3Aretweet%20result_type%3Drecent%20since%3D2021-03-01"
result = api.GetSearch("covid", since="2021-03-01", result_type="mixed")

print(result)
# producer = KafkaProducer()

# future = producer.send('test-topic', b'Time do asses eh o melhor da Dell/Atlantico')