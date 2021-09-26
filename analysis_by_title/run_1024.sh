$HADOOP_HOME/bin/hdfs dfs -rm -r output

$SPARK_HOME/bin/spark-submit --master local[*] /home/simone/Scrivania/Big-Data-Final-Project/big-data-final-project/analysis_by_title/analysis_by_title.py \
--input_path_1 hdfs:///user/simone/input/title_akas1024.tsv \
--input_path_2 hdfs:///user/simone/input/title_basics1024.tsv \
--output_path hdfs:///user/simone/output
