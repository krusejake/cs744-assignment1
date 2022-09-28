setenv PATH $PATH":/users/Yuhan/hadoop-3.3.4/bin"
hadoop fs -mkdir /input
hadoop fs -mkdir /output

wget https://snap.stanford.edu/data/web-BerkStan.txt.gz
gunzip web-BerkStan.txt.gz
hadoop fs -put web-BerkStan.txt /input
hadoop fs -put /proj/uwmadison744-f22-PG0/data-part3/enwiki-pages-articles /input 

spark-3.3.0-bin-hadoop3/bin/spark-submit pagerank_wiki_4.py