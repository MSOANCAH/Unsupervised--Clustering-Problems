from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient
from tweepy.streaming import StreamListener

MONGO_CONN = 'mongodb://localhost:27017/'
WORDS = ['#INDvsSL', '#SLvIND','2ndODI']

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''


class MyStreamListener(StreamListener):


    def on_connect(self):
        print("Connected")

    def on_error(self, status_code):
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        try:
            client = MongoClient(MONGO_CONN)
            db = client.twittercricketdb

            with open('C:/Users/Madhu/Desktop/cricket.json', 'a') as f:
                f.write(data)
                f.write('\n')

            datajson = json.loads(data)
            created_at = datajson['created_at']
            print("Tweet collected at " + str(created_at))

            db.twitter_cricket_coll.insert(datajson)
            print('working')
        except Exception as e:
            print(e)


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

listener = MyStreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)

print("Tracking: " + str(WORDS))
streamer.filter(track=WORDS)



