#!/usr/bin/env python3
"""spark application"""
import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path_1", type=str, help="Input file path for title.principals.tsv")
parser.add_argument("--input_path_2", type=str, help="Input file path for name.basics.tsv")
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

title_principals = sc.textFile(input_filepath_1).cache().distinct().map(lambda line: line.strip().split('\t'))
name_basics = sc.textFile(input_filepath_2).cache().distinct().map(lambda line: line.strip().split('\t'))

# ((nconst), (tconst, rdering, category, job, characters))
title_principals_nconst = title_principals.map(lambda line: ((line[2]), (line[0], line[1], line[3], line[4], line[5])))

# ((nconst), (primaryName, birthYear, deathYear, primaryProfession, knownForTitles))
name_basics_nconst = name_basics.map(lambda line: ((line[0]), (line[1], line[2], line[3], line[4], line[5])))

# (nconst, ((primaryName, birthYear, deathYear, primaryProfession, knownForTitles), (tconst, ordering, category, job, characters)))
cast_info = name_basics_nconst.join(title_principals_nconst).distinct()

# (nconst, (primaryName, tconst, knownForTitles))
cast_info_uv = cast_info.map(lambda line: (line[0], (line[1][0][0], line[1][1][0], line[1][0][4])))

# (nconst, (primaryName, list of tconst, knownForTitles))
actor_titles = cast_info_uv.reduceByKey(lambda a, b: (a[0], a[1] + "," + b[1], a[2]))

# (nconst, (primaryName, num of tconst, knownForTitles))
actor_titles_count = actor_titles.map(lambda line: (line[0], (line[1][0], len(line[1][1].split(",")), line[1][2])))

# Save and print
actor_titles_count.sortBy(keyfunc=lambda x: x[1][1], ascending=False).saveAsTextFile(output_filepath)
