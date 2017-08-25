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


# Data preprocessing
import codecs
import time
import sys
import re

def normalize_text(text):
    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(pic\.twitter\.com/[^\s]+))','', text)
    text = re.sub('@[^\s]+','', text)
    text = re.sub('#([^\s]+)', '', text)
    text = re.sub('[:;>?<=*+()/,\-#!$%\{˜|\}\[^_\\@\]1234567890’‘]',' ', text)
    text = re.sub('[\d]','', text)
    text = text.replace(".", '')
    text = text.replace("'", ' ')
    text = text.replace("\"", ' ')
    #normalize some utf8 encoding
    text = text.replace("\x9d",' ').replace("\x8c",' ')
    text = text.replace("\xa0",' ')
    text = text.replace("\x9d\x92", ' ').replace("\x9a\xaa\xf0\x9f\x94\xb5", ' ').replace("\xf0\x9f\x91\x8d\x87\xba\xf0\x9f\x87\xb8", ' ').replace("\x9f",' ').replace("\x91\x8d",' ')
    text = text.replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8",' ').replace("\xf0",' ').replace('\xf0x9f','').replace("\x9f\x91\x8d",' ').replace("\x87\xba\x87\xb8",' ')
    text = text.replace("\xe2\x80\x94",' ').replace("\x9d\xa4",' ').replace("\x96\x91",' ').replace("\xe1\x91\xac\xc9\x8c\xce\x90\xc8\xbb\xef\xbb\x89\xd4\xbc\xef\xbb\x89\xc5\xa0\xc5\xa0\xc2\xb8",' ')
    text = text.replace("\xe2\x80\x99s", " ").replace("\xe2\x80\x98", ' ').replace("\xe2\x80\x99", ' ').replace("\xe2\x80\x9c", " ").replace("\xe2\x80\x9d", " ")
    text = text.replace("\xe2\x82\xac", " ").replace("\xc2\xa3", " ").replace("\xc2\xa0", " ").replace("\xc2\xab", " ").replace("\xf0\x9f\x94\xb4", " ").replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8\xf0\x9f", "")
    return text

def parse_json_tweet(line):
    tweet = json.loads(line)
    if tweet['lang'] != 'en':
        # print "non-english tweet:", tweet['lang'], tweet
        return ['', '', '', [], [], []]

    date = tweet['created_at']
    id = tweet['id']

    if 'retweeted_status' in tweet:
        text = tweet['retweeted_status']['text']
    else:
        text = tweet['text']


    hashtags = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]

    return [date, id, text, hashtags]

file_timeordered_json_tweets = codecs.open(sys.argv[1], 'r', 'utf-8')
fout = codecs.open(sys.argv[2], 'w', 'utf-8')

for line in file_timeordered_json_tweets:
    try:

        [tweet_gmttime, tweet_id, text1, hashtags] = parse_json_tweet(
            line)
        t = normalize_text(text1)
        print(t)
        try:
            c = time.strptime(tweet_gmttime.replace("+0000", ''), '%a %b %d %H:%M:%S %Y')
        except:
            print("pb with tweet_gmttime", tweet_gmttime, line)
            pass
        tweet_unixtime = int(time.mktime(c))

    except:
        pass


file_timeordered_json_tweets.close()
fout.close()