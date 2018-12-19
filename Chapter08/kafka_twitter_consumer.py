#!/usr/bin/python

""" kafka_twitter_consumer.py: Consume Tweets from Apache Kafka and classify their Sentiment in Real-Time """

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import LongType, DoubleType, IntegerType, StringType, BooleanType
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassificationModel

# (1) Import our Config file and our Pre-Processing and Feature Vectorisation pipeline functions
import config
import model_pipelines

__author__ = "Jillur Quddus"
__credits__ = ["Jillur Quddus"]
__version__ = "1.0.0"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@keisan.io"
__status__ = "Development"

# (2) Create a Spark Session using the Spark Context instantiated from spark-submit
spark = SparkSession.builder.appName("Stream Processing - Real-Time Sentiment Analysis").getOrCreate()

# (3) Load the Trained Decision Tree Classifier that we trained and persisted in Chapter 06
decision_tree_model = DecisionTreeClassificationModel.load(config.trained_classification_model_path)

# (4) Spark Structured Streaming does not yet support the automatic inference of JSON Kafka values into a Dataframe without a Schema
# Therefore let us define the Schema explicitly
schema = StructType([
    StructField("created_at", StringType()), 
    StructField("id", StringType()), 
    StructField("id_str", StringType()), 
    StructField("text", StringType()), 
    StructField("retweet_count", StringType()), 
    StructField("favorite_count", StringType()), 
    StructField("favorited", StringType()), 
    StructField("retweeted", StringType()), 
    StructField("lang", StringType()), 
    StructField("location", StringType()) 
])

# (5) Subscribe to the Kafka Twitter Topic and consume binary Kafka Records into an unbounded Spark DataFrame
tweets_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", config.bootstrap_servers) \
            .option("subscribe", config.twitter_kafka_topic_name) \
            .load()

# (6) Cast the Key and Value (containing our Tweet Record) fields as STRING from their default BINARY
# Then apply the defined Schema to parse the Value String as JSON
# Finally extract the fields that we require for our model, namely tweet.id and tweet.text
tweets_df = tweets_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json") \
                .withColumn("tweet", from_json(col("json"), schema=schema)) \
                .selectExpr("tweet.id_str as id", "tweet.text as text")

# (7) Apply the Pre-Processing Pipeline to these Tweets
preprocessed_df = model_pipelines.preprocessing_pipeline(tweets_df)

# (8) Apply the Feature Vectorization Pipeline to these Pre-Processed Tweets (Tokens)
features_df = model_pipelines.vectorizer_pipeline(preprocessed_df)

# (9) Apply the Trained Decision Tree Classifier to the Pre-Processed Feature Vectors to predict and classify sentiment
predictions_df = decision_tree_model.transform(features_df).select("id", "text", "prediction")

# (10) Output the results of the Streaming Transformations and final predicted sentiments to the console sink
query = predictions_df.writeStream.outputMode("complete").format("console").option("truncate", "false").start() 
query.awaitTermination()
