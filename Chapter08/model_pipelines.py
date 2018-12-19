#!/usr/bin/python

""" model_pipelines.py: Pre-Processing and Feature Vectorization Spark Pipeline function definitions """

from pyspark.sql.functions import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF
from pyspark.ml import Pipeline, PipelineModel

from sparknlp.base import *
from sparknlp.annotator import Tokenizer as NLPTokenizer
from sparknlp.annotator import Stemmer, Normalizer

__author__ = "Jillur Quddus"
__credits__ = ["Jillur Quddus"]
__version__ = "1.0.0"
_maintainer__ = "Jillur Quddus"
__email__ = "jillur.quddus@keisan.io"
__status__ = "Development"

def preprocessing_pipeline(raw_corpus_df):
    
    """ Pre-process a raw corpus of documents by defining a Pipeline consisting of the
    following feature transformer stages:
    
        1. MLlib Tokenizer
        2. MLlib Stop-Words Remover
        3. spark-nlp Stemmer
        4. spark-nlp Normalizer
        
    """
    
    # Native MLlib Feature Transformers
    filtered_df = raw_corpus_df.filter("text is not null")
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens_1")
    tokenized_df = tokenizer.transform(filtered_df)
    remover = StopWordsRemover(inputCol="tokens_1", outputCol="filtered_tokens")
    preprocessed_part_1_df = remover.transform(tokenized_df)
    preprocessed_part_1_df = preprocessed_part_1_df.withColumn("concatenated_filtered_tokens", 
                                                               concat_ws(" ", col("filtered_tokens")))
    
    # spark-nlp Feature Transformers
    document_assembler = DocumentAssembler().setInputCol("concatenated_filtered_tokens")
    tokenizer = NLPTokenizer().setInputCols(["document"]).setOutputCol("tokens_2")
    stemmer = Stemmer().setInputCols(["tokens_2"]).setOutputCol("stems")
    normalizer = Normalizer().setInputCols(["stems"]).setOutputCol("normalised_stems")
    preprocessing_pipeline = Pipeline(stages=[document_assembler, tokenizer, stemmer, normalizer])
    preprocessing_pipeline_model = preprocessing_pipeline.fit(preprocessed_part_1_df)
    preprocessed_df = preprocessing_pipeline_model.transform(preprocessed_part_1_df)
    preprocessed_df.select("id", "text", "normalised_stems")
    
    # Explode and Aggregate
    exploded_df = preprocessed_df.withColumn("stems", explode("normalised_stems")) \
        .withColumn("stems", col("stems").getItem("result")).select("id", "text", "stems")
    aggregated_df = exploded_df.groupBy("id").agg(concat_ws(" ", collect_list(col("stems"))), first("text")) \
                    .toDF("id", "tokens", "text") \
                    .withColumn("tokens", split(col("tokens"), " ").cast("array<string>"))
    
    # Return the final processed DataFrame
    return aggregated_df

def vectorizer_pipeline(preprocessed_df):
    
    """ Generate Feature Vectors from the Pre-processed corpus using the 
    hashingTF transformer on the filtered, stemmed and normalised list of Tokens
    """
    
    hashingTF = HashingTF(inputCol="tokens", outputCol="features", numFeatures=280)
    features_df = hashingTF.transform(preprocessed_df)
    
    # Return the final vectorized DataFrame
    return features_df

