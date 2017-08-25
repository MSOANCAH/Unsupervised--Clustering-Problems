from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient
from tweepy.streaming import StreamListener

MONGO_CONN = 'mongodb://localhost:27017/'
WORDS = ['#INDvsSL', '#SLvIND','2ndODI']

consumer_key = ''
consumer_secret = ''
access_token = '-'
access_secret = ''

#stemming the data
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

            with open('C:/Users/Madhu/Desktop/cricketstreamming.json', 'a') as f:
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

#events modelling
events=[]
toss=[]
toss_tags=['#elects','#elect','#bat','#bowl','#tosstime']
toss_search=['elected to bat',' win the toss ','wins the toss','elects to bat','elected to bowl','elected to field']
toss.append(toss_tags)
toss.append(toss_search)
events.append(toss)

out=[]
out_tags=['#wicket','#bowled','#duckout','#lbw']
out_search=['wicket falling','another wicket down','lbw','hitwicket','catchout','walking back pavilion']
out.append(out_tags)
out.append(out_search)
events.append(out)

boundery=[]
boundery_tags=['#boundery','#4runs','#four']
boundery_search=['four runs','hits a four','4runs']
boundery.append(boundery_tags)
boundery.append(boundery_search)
events.append(boundery)

six=[]
six_tags=['#sixruns','#6runs','#sixer']
six_search=['sixer','six runs','smashed a six','6runs in 1 ball','no ball six runs','huge six','maximum six','more sixes']
six.append(six_tags)
six.append(six_search)
events.append(six)

scoreboard=[]
scoreboard_tags=['#scoreboard']
scoreboard_search=['scoreboard','need more runs to win','powerplay runs','highest runs']
scoreboard.append(scoreboard_tags)
scoreboard.append(scoreboard_search)
events.append(scoreboard)

amazing_moments=[]
amazing_moments_search=['awesome catch','big six','back to back wickets','powerplay runs','century','half cenury','great vicort,'
                        'amazing over']
amazing_moments_tags=[]
amazing_moments.append(amazing_moments_tags)
amazing_moments.append(amazing_moments_search)
events.append(amazing_moments)

extra_runs=[]
extra_runs_tags=['#wide','#noball','#4wides','#4noballs','#extraruns','#legby']
extra_runs_search=['wide ball','no ball','extra runs','four wides','freehit']
extra_runs.append(extra_runs_tags)
extra_runs.append(extra_runs_search)
events.append(extra_runs)


#get data from twitter based on events
api= tweepy.API(auth)
for event in events:
    for type_t_s in event:
        for text in type_t_s:
            for status in tweepy.Cursor(api.search, q=text).items(1):
                # Process a single status
                json_str = json.dumps(status._json)
                print(json_str)
                with open('C:/Users/Madhu/Desktop/cricketmodelling.json', 'a') as f:
                    f.write(json_str)
                    f.write('\n')


# Twitter Data text collecting
import codecs
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

file_model_json_tweets = codecs.open(sys.argv[1], 'r', 'utf-8')

tlist=[]
tweet_id_list=[]
tweet_gmttime_list=[]
for line in file_model_json_tweets:
    try:

        [tweet_gmttime, tweet_id, text1, hashtags] = parse_json_tweet(line)
        tweet_gmttime_list.append(tweet_gmttime)
        tweet_id_list.append(tweet_id)
        t = normalize_text(text1)
        tlist.append(t)

    except:
        pass


file_model_json_tweets.close()


#feature extraction and clustering
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

vectorizer = TfidfVectorizer(stop_words='english')
X = vectorizer.fit_transform(tlist)

true_k = 7
model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
model.fit(X)

print("Top terms per cluster:")
order_centroids = model.cluster_centers_.argsort()[:, ::-1]
terms = vectorizer.get_feature_names()
for i in range(true_k):
    print("Cluster %d:" % i),
    for ind in order_centroids[i, :10]:
        print(' %s' % terms[ind])


print("\n")
print("Cluster Prediction for streaming data")

#streammingthe twitter data
listener = MyStreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
streamer.filter(track=WORDS)

file_streaming_json_tweets = codecs.open(sys.argv[2], 'r', 'utf-8')

tweetlist_stream=[]
tweet_id_list_stream=[]
tweet_gmttime_list_stream=[]
for line in file_streaming_json_tweets:
    try:

        [tweet_gmttime, tweet_id, text1, hashtags] = parse_json_tweet(
            line)
        tweet_gmttime_list_stream.append(tweet_gmttime)
        tweet_id_list_stream.append(tweet_id)
        t = normalize_text(text1)
        tweetlist_stream.append(t)

    except:
        pass


file_streaming_json_tweets.close()


for tweet in tweetlist_stream:
    Y = vectorizer.transform(tweet)
    prediction = model.predict(Y)
    print(prediction)



