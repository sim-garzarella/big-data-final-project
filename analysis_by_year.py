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
#input_filepath_1 = args.input_path_1
#input_filepath_2 = args.input_path_2
input_filepath_3 = args.input_path_3
#input_filepath_4 = args.input_path_4
#input_filepath_5 = args.input_path_5
output_filepath = args.output_path

# initialize SparkSession
# with the proper configuration
spark = SparkSession.builder.appName("Big Data Final Project").getOrCreate()

sc = spark.sparkContext


def element_count(input_list):

    element_to_count = {}

    for element in input_list:
        if element in element_to_count:
            element_to_count[element] += 1
        else:
            element_to_count[element] = 1

    return element_to_count


# name_basics = sc.textFile(input_filepath_1).cache().map(lambda line: line.strip().split('\t'))
# title_akas = sc.textFile(input_filepath_2).cache().map(lambda line: line.strip().split('\t'))
title_basics = sc.textFile(input_filepath_3).cache().map(lambda line: line.strip().split('\t'))
# title_principals = sc.textFile(input_filepath_4).cache().map(lambda line: line.strip().split('\t'))
# title_ratings = sc.textFile(input_filepath_5).cache().map(lambda line: line.strip().split('\t'))

year_types_genres = title_basics.map(lambda line: ((line[5]), (line[1], line[8])))

year_all_types = year_types_genres.map(lambda line: ((line[0]), line[1][0])).reduceByKey(lambda a, b: a + "," + b)

year_all_genres = year_types_genres.map(lambda line: ((line[0]), line[1][1])).reduceByKey(lambda a, b: a + "," + b)

year_types_count = year_all_types.map(lambda line: (("Year: " + line[0]), ("Types: ", element_count(line[1].split(",")))))

year_genre_count = year_all_genres.map(lambda line: (("Year: " + line[0]), ("Genres: ", element_count(line[1].split(",")))))

year_genre_types_count = year_types_count.join(year_genre_count)

year_genre_types_count.sortBy(keyfunc=lambda x: x[0], ascending=True).saveAsTextFile(output_filepath)
