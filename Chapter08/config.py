#!/usr/bin/python

""" config.py: Environmental and Application Settings """

""" ENVIRONMENT SETTINGS """

# Apache Kafka
bootstrap_servers = '192.168.56.10:9092'
data_encoding = 'utf-8'

""" TWITTER APP SETTINGS """

consumer_api_key = 'Enter your Twitter App Consumer API Key here'
consumer_api_secret = 'Enter your Twitter App Consumer API Secret Key here'
access_token = 'Enter your Twitter App Access Token here'
access_token_secret = 'Enter your Twitter App Access Token Secret here'

""" SENTIMENT ANALYSIS MODEL SETTINGS """

# Name of an existing Kafka Topic to publish tweets to
twitter_kafka_topic_name = 'twitter'

# Keywords, Twitter Handle or Hashtag used to filter the Twitter Stream
twitter_stream_filter = '@British_Airways'

# Filesystem Path to the Trained Decision Tree Classifier
trained_classification_model_path = '/data/workspaces/jillur.quddus/jupyter/notebooks/Machine-Learning-with-Apache-Spark-QuickStart-Guide/chapter06/models/airline-sentiment-analysis-decision-tree-classifier'
