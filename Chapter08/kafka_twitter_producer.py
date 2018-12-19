#!/usr/bin/python

""" kafka_twitter_producer.py: Stream Tweets consumed from the Twitter API and publish to an Apache Kafka Topic """

# (1) Import Tweepy, PyKafka and our Config file
import config
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import pykafka

__author__ = "Jillur Quddus"
__credits__ = ["Jillur Quddus"]
__version__ = "1.0.0"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@keisan.io"
__status__ = "Development"

# (2) Instantiate the Tweepy Twitter API Wrapper using your Twitter App Authentication Credentials (see config.py)
auth = OAuthHandler(config.consumer_api_key, config.consumer_api_secret)
auth.set_access_token(config.access_token, config.access_token_secret)
api = tweepy.API(auth)

class KafkaTwitterProducer(StreamListener):
    
    """ (3) Define our Kafka Twitter Producer Client """
    
    def __init__(self):
        
        """ Instantiate a PyKafka Client and a subsequent Producer subscribed 
        to the designated Kafka Topic (as defined in config.py)
        """
        
        self.client = pykafka.KafkaClient(config.bootstrap_servers)
        self.producer = self.client.topics[bytes(config.twitter_kafka_topic_name, 
                                                 config.data_encoding)].get_producer()
        
    def on_data(self, data):
        
        """ Publish data to the Kafka Topic (using a data encoding as defined in config.py) """
        
        self.producer.produce(bytes(data, config.data_encoding))
        return True
    
    def on_error(self, status):
        print(status)
        return True

# (4) Instantiate a Twitter Stream and publish to Kafka using an instance of our KafkaTwitterProducer Class
print("Instantiating a Twitter Stream and publishing to the '%s' Kafka Topic..." % config.twitter_kafka_topic_name)
twitter_stream = Stream(auth, KafkaTwitterProducer())

# (5) Filter the Twitter Stream based on the chosen Keywords, Twitter Handle or Hashtag (as defined in config.py)
print("Filtering the Twitter Stream based on the query '%s'..." % config.twitter_stream_filter)
twitter_stream.filter(track=[config.twitter_stream_filter])

