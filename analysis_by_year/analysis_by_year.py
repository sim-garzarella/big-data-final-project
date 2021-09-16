#!/usr/bin/env python3
"""spark application"""
import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path for title.basics.tsv")
parser.add_argument("--output_path", type=str, help="Output folder")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path
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


def total_count(input_map):
    tot = 0

    for element in input_map:
        tot += input_map[element]

    return str(tot)


title_basics = sc.textFile(input_filepath).cache().distinct().map(lambda line: line.strip().split('\t'))

year_types_genres_isAdult = title_basics.map(lambda line: ((line[5]), (line[1], line[8], line[4])))

year_all_types = year_types_genres_isAdult.map(lambda line: ((line[0]), line[1][0])).reduceByKey(lambda a, b: a + "," + b)

year_all_genres = year_types_genres_isAdult.map(lambda line: ((line[0]), line[1][1])).reduceByKey(lambda a, b: a + "," + b)

year_isAdult = year_types_genres_isAdult.map(lambda line: ((line[0]), line[1][2])).reduceByKey(lambda a, b: a + "," + b)

year_types_count = year_all_types.map(lambda line: (("Year: " + line[0]), ("Total: " + total_count(element_count(line[1].split(","))), "Types: ", element_count(line[1].split(",")))))

year_genre_count = year_all_genres.map(lambda line: (("Year: " + line[0]), ("Genres: ", element_count(line[1].split(",")))))

year_isAdult_count = year_isAdult.map(lambda line: (("Year: " + line[0]), ("Is Adult: ", element_count(line[1].split(",")))))

year_genre_types_count = year_types_count.join(year_genre_count).join(year_isAdult_count)

year_genre_types_count.sortBy(keyfunc=lambda x: x[0], ascending=True).saveAsTextFile(output_filepath)
