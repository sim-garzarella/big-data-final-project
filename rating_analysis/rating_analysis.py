#!/usr/bin/env python3
"""spark application"""
import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path_1", type=str, help="Input file path for title.basics.tsv")
parser.add_argument("--input_path_2", type=str, help="Input file path for name.ratings.tsv")
parser.add_argument("--output_path", type=str, help="Output folder")

# parse arguments
args = parser.parse_args()
input_filepath_1 = args.input_path_1
input_filepath_2 = args.input_path_2
output_filepath = args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession.builder.appName("Big Data Final Project").getOrCreate()

sc = spark.sparkContext

title_basics = sc.textFile(input_filepath_1).cache().distinct().map(lambda line: line.strip().split('\t'))
title_ratings = sc.textFile(input_filepath_2).cache().distinct().map(lambda line: line.strip().split('\t'))



# Save and print
title_ratings.sortBy(keyfunc=lambda x: x[0], ascending=True).saveAsTextFile(output_filepath)
