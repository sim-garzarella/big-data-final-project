$HADOOP_HOME/bin/hdfs dfs -rm -r output

$SPARK_HOME/bin/spark-submit --master local[*] /home/simone/Scrivania/Big-Data-Final-Project/big-data-final-project/analysis_by_year/analysis_by_year.py \
--input_path hdfs:///user/simone/input/title_basics256.tsv \
--output_path hdfs:///user/simone/output
