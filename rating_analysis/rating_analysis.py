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


def distributeRating(avgRating, genres):

    ditributed_rating = ""

    for genre in genres:
        ditributed_rating = ditributed_rating + genre + "|" + avgRating + ","

    return ditributed_rating[:-1]


def reduceRating(genre_list):  # Input: ["Comedy|4.2", "Short|4.2", "Short|4.5", "Short|3.9", "Short|3.7", "Sport|3.7", "Documentary|4.6", "Short|4.6"]

    genre_2_rating = {}

    for element in genre_list:
        genre, rating = element.split("|")

        rating = float(rating)

        if genre not in genre_2_rating:
            genre_2_rating[genre] = [rating]
        else:
            genre_2_rating[genre].append(rating)

    for element in genre_2_rating:
        genre_2_rating[element] = sum(genre_2_rating[element]) / len(genre_2_rating[element])
        genre_2_rating[element] = round(genre_2_rating[element], 2)

        print(genre_2_rating[element])

    return genre_2_rating


title_basics = sc.textFile(input_filepath_1).cache().distinct().map(lambda line: line.strip().split('\t')).map(lambda line: ((line[0]), (line[2], line[5], line[8]))).filter(lambda line: line[0] != "tconst")
title_ratings = sc.textFile(input_filepath_2).cache().distinct().map(lambda line: line.strip().split('\t')).map(lambda line: ((line[0]), (line[1], line[2]))).filter(lambda line: line[0] != "tconst")


# (tconst), ((averageRating, numVotes), (primaryTitle, startYear, genres))
title_ratings_info = title_ratings.join(title_basics).distinct()

# (year), (avgRatings, genres)
year_avgRatings_genres = title_ratings_info.map(lambda line: ((line[1][1][1]), (line[1][0][0], line[1][1][2]))).filter(lambda line: line[1][0] != "\\N").filter(lambda line: line[1][1] != "\\N")

# (year), (ditributedAvg) // Es. (1999), ("Horror|7,Musical|7,Fantasy|7")
year_ditributedAvg = year_avgRatings_genres.map(lambda line: ((line[0]), (distributeRating(line[1][0], line[1][1].split(",")))))

# (year), (ditributed genres with avg) // Es. (1896), ("Comedy|4.2,Short|4.2,Short|4.5,Short|3.9,Short|3.7,Sport|3.7,Documentary|4.6,Short|4.6")
year_ditributed_genresAvg = year_ditributedAvg.reduceByKey(lambda a, b: a + "," + b)

results = year_ditributed_genresAvg.map(lambda line: ((line[0]), reduceRating(line[1].split(","))))

# Save and print
results.sortBy(keyfunc=lambda x: x[0], ascending=True).saveAsTextFile(output_filepath)
