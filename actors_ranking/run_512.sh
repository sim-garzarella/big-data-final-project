$HADOOP_HOME/bin/hdfs dfs -rm -r output

$SPARK_HOME/bin/spark-submit --master local[*] /home/simone/Scrivania/Big-Data-Final-Project/big-data-final-project/actors_ranking/actors_ranking.py \
--input_path_1 hdfs:///user/simone/input/title_principals512.tsv \
--input_path_2 hdfs:///user/simone/input/name_basics512.tsv \
--output_path hdfs:///user/simone/output
