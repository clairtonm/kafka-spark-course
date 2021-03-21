#from kafka import KafkaProducer
import twitter
import configparser

config = configparser.ConfigParser()
config.read('./twitter.ini')

api = twitter.Api(consumer_key=config.get('twitter', 'API_KEY'),
                  consumer_secret=config.get('twitter', 'API_TOKEN'),
                  access_token_key=config.get('twitter', 'ACCESS_TOKEN'),
                  access_token_secret=config.get('twitter', 'ACCESS_TOKEN_SECRET'))

query = "q=(#covid OR #covid19) has:lang has:geo -is:retweet result_type=recent since=2021-03-01"
enconded_query = r"q%3D%28%23covid%20OR%20%23covid19%29%20has%3Alang%20has%3Ageo%20-is%3Aretweet%20result_type%3Drecent%20since%3D2021-03-01"
result = api.GetSearch("covid", since="2021-03-01", result_type="mixed")

print(result)
# producer = KafkaProducer()

# future = producer.send('test-topic', b'Time do asses eh o melhor da Dell/Atlantico')