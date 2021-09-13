$HADOOP_HOME/bin/hdfs dfs -rm -r output

$SPARK_HOME/bin/spark-submit --master local[*] /home/simone/Scrivania/Big-Data-Final-Project/big-data-final-project/analysis_by_year.py \
--input_path_1 hdfs:///user/simone/input/name_basics256.tsv \
--input_path_2 hdfs:///user/simone/input/title_akas256.tsv \
--input_path_3 hdfs:///user/simone/input/title_basics256.tsv \
--input_path_4 hdfs:///user/simone/input/title_principals256.tsv \
--input_path_5 hdfs:///user/simone/input/title_ratings256.tsv \
--output_path hdfs:///user/simone/output
