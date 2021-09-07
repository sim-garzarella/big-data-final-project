#!/usr/bin/env python3
"""spark application"""
import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path_1", type=str, help="Input file path for name.basics.tsv")
parser.add_argument("--input_path_2", type=str, help="Input file path for title.akas.tsv")
parser.add_argument("--input_path_3", type=str, help="Input file path for title.basics.tsv")
parser.add_argument("--input_path_4", type=str, help="Input file path for title.principals.tsv")
parser.add_argument("--input_path_5", type=str, help="Input file path for title.ratings.tsv")
parser.add_argument("--output_path", type=str, help="Output folder")

# parse arguments
args = parser.parse_args()
input_filepath_1, input_filepath_2, input_filepath_3, input_filepath_4, input_filepath_5, output_filepath = args.input_path_1, args.input_path_2, args.input_path_3, args.input_path_4, args.input_path_5, args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession.builder.appName("Big Data Final Project").getOrCreate()

sc = spark.sparkContext

# data = spark.read.option("header", "true") \
#     .option("sep", "\t") \
#     .option("multiLine", "true") \
#     .option("quote", "\"") \
#     .option("escape", "\"") \
#     .option("ignoreTrailingWhiteSpace", "true") \
#     .csv(input_filepath_1)

name_basics = sc.textFile(input_filepath_1).cache().map(lambda line: line.strip().split('\t'))
title_akas = sc.textFile(input_filepath_2).cache().map(lambda line: line.strip().split('\t'))
title_basics = sc.textFile(input_filepath_3).cache().map(lambda line: line.strip().split('\t'))
title_principals = sc.textFile(input_filepath_4).cache().map(lambda line: line.strip().split('\t'))
title_ratings = sc.textFile(input_filepath_5).cache().map(lambda line: line.strip().split('\t'))

name_basics.saveAsTextFile(output_filepath)
