setenv PATH $PATH":/users/Yuhan/hadoop-3.3.4/bin"
hadoop fs -mkdir /input
hadoop fs -mkdir /output

http://pages.cs.wisc.edu/~shivaram/cs744-fa18/assets/export.csv
hadoop fs -put export.csv /input

spark-3.3.0-bin-hadoop3/bin/spark-submit part2.py