import json
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
import tweepy
import configparser
import kafka
import json

from nltk.tokenize import WordPunctTokenizer
import re
from bs4 import BeautifulSoup


token = WordPunctTokenizer()
patern1 = r'@[A-Za-z0-9]+'
patern2 = r'https?://[A-Za-z0-9./]+'
combinedpatern = r'|'.join((patern1, patern2))
www_patern = r'www.[^ ]+'
negativedic = {"isn't":"is not", "aren't":"are not", "wasn't":"was not", "weren't":"were not",
                "haven't":"have not","hasn't":"has not","hadn't":"had not","won't":"will not",
                "wouldn't":"would not", "don't":"do not", "doesn't":"does not","didn't":"did not",
                "can't":"can not","couldn't":"could not","shouldn't":"should not","mightn't":"might not",
                "mustn't":"must not"}
negativepatern = re.compile(r'\b(' + '|'.join(negativedic.keys()) + r')\b')


def tweet_preprocessing(tweet):
    soup = BeautifulSoup(tweet, 'lxml')
    soup = soup.get_text()
    try:
        clean = soup.decode("utf-8-sig").replace(u"\ufffd", "?")
    except:
        clean = soup
    rip = re.sub(combinedpatern, '', clean)
    rip = re.sub(www_patern, '', rip)
    lower = rip.lower()
    neg = negativepatern.sub(lambda x: negativedic[x.group()], lower)
    alpha= re.sub("[^a-zA-Z]", " ", neg)
    w = [w for w  in token.tokenize(alpha) if len(w) > 1]
    return (" ".join(w)).strip()



class KafkaStreamListener(tweepy.StreamListener):

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        self.producer = kafka.KafkaProducer(bootstrap_servers="localhost:9092")

    def on_status(self, status):

        msg =  tweet_preprocessing(status.text)


        try:
            self.producer.send("test",json.dumps({'tweet': msg, "Target": 0}).encode('UTF-8','ignore'))

        except Exception as e:
            print(e)
            return False
        return True


    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True

    def on_timeout(self):
        return True

if __name__ == '__main__':


    consumer_key = 'insert API credentials'
    consumer_secret = 'insert API credentials'
    access_key = 'insert API credentials'
    access_secret = 'insert API credentials'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    stream = tweepy.Stream(auth, listener = KafkaStreamListener(api))


    stream.filter(locations=[-180,-90,180,90], languages = ['en'])
