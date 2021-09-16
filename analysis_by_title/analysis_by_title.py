#!/usr/bin/env python3
"""spark application"""
import argparse

# create parser and set its arguments
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input_path_1", type=str, help="Input file path for title.akas.tsv")
parser.add_argument("--input_path_2", type=str, help="Input file path for title.basics.tsv")
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


# def element_count(input_list):
#
#     element_to_count = {}
#
#     for element in input_list:
#         if element in element_to_count:
#             element_to_count[element] += 1
#         else:
#             element_to_count[element] = 1
#
#     return element_to_count
#
#
# def total_count(input_map):
#     tot = 0
#
#     for element in input_map:
#         tot += input_map[element]
#
#     return str(tot)


title_akas = sc.textFile(input_filepath_1).cache().distinct().map(lambda line: line.strip().split('\t'))

title_basics = sc.textFile(input_filepath_2).cache().distinct().map(lambda line: line.strip().split('\t'))

# (titleID), (region, language, type)
title_akas_uv = title_akas.map(lambda line: ((line[0]), (line[3], line[4], line[5])))

# (titleID), (titleType, primaryTitle, year, minutes)
title_basics_uv = title_basics.map(lambda line: ((line[0]), (line[1], line[2], line[5], line[7])))

# (titleID), ((region, language, type), (titleType, primaryTitle, year, minutes))
title_info = title_akas_uv.join(title_basics_uv)

# (titleID), (primaryTitle, region)
title_region = title_info.map(lambda line: ((line[0]), (line[1][1][1], line[1][0][0]))).filter(lambda line: line[1][1] != "\\N").distinct()

# (titleID), (primaryTitle, regions)
title_regions = title_region.reduceByKey(lambda a, b: (a[0], a[1] + "," + b[1]))

# (titleID), (primaryTitle, total number of regions, regions)
title_count_regions = title_regions.map(lambda line: ((line[0]), (line[1][0], "Number of Regions: " + str(len(line[1][1].split(","))), "Regions: " + line[1][1])))

# (titleID), (primaryTitle, language)
title_language = title_info.map(lambda line: ((line[0]), (line[1][1][1], line[1][0][1]))).filter(lambda line: line[1][1] != "\\N").distinct()

# (titleID), (primaryTitle, languages)
title_languages = title_language.reduceByKey(lambda a, b: (a[0], a[1] + "," + b[1]))

title_count_languages = title_languages.map(lambda line: ((line[0]), (line[1][0], "Number of Languages: " + str(len(line[1][1].split(","))), "Languages: " + line[1][1])))

title_count_languages.sortBy(keyfunc=lambda x: x[0], ascending=True).saveAsTextFile(output_filepath)
