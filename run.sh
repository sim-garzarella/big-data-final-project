$HADOOP_HOME/bin/hdfs dfs -rm -r output

$SPARK_HOME/bin/spark-submit --master local /home/simone/Scrivania/Big-Data-Progetto-Finale/app.py --input_path_1 hdfs:///user/simone/input/name.basics.tsv --input_path_2 hdfs:///user/simone/input/title.akas.tsv --input_path_3 hdfs:///user/simone/input/title.basics.tsv --input_path_4 hdfs:///user/simone/input/title.principals.tsv --input_path_5 hdfs:///user/simone/input/title.ratings.tsv --output_path hdfs:///user/simone/output/results.txt

