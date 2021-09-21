$HADOOP_HOME/bin/hdfs dfs -rm -r output

$SPARK_HOME/bin/spark-submit --master local[*] /home/simone/Scrivania/Big-Data-Final-Project/big-data-final-project/rating_analysis/rating_analysis.py \
--input_path_1 hdfs:///user/simone/input/title.basics.tsv \
--input_path_2 hdfs:///user/simone/input/title.ratings.tsv \
--output_path hdfs:///user/simone/output

