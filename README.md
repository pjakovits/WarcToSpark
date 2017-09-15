# WarcToSpark

Spark Java Program to extract HTML contents from WARC archives for the estnltk project (https://github.com/peepkungas/estnltk-openstack-spark). 

Contains two different Spark jobs:
1. A standalone batch proccessing jo: org.ut.scicloud.estnltk.SparkWarcReader
2. Spark Straming job: org.ut.scicloud.estnltk.SparkWarcStreamingReader 

Output is in the Hadoop SequenceFile format and contains the following values: \
Text key - protocol + "::" + hostname + "::" + urlpath + "::" + parameters + "::" + date ("yyyymmddhhmmss") \
Text val - raw html string 

To make the output of the program compatible with text based Python Spark streaming, all linebreaks are rmoved from the HTML to make it a single line sting. \
In addition, we are using "*_*_*_*_*_*_*_*_" as a separator between Key and Value fields, which should be configured accordingly in any program which automatically parses the output of this process. 

Command to test streaming job locally:

spark-submit --master local[4] \
--jars lib/warcutils-1.2.jar,\
lib/jwat-archive-common-1.0.5.jar,\
lib/jwat-common-1.0.5.jar,\
lib/jwat-warc-1.0.5.jar \
--class org.ut.scicloud.estnltk.SparkWarcStreamingReader \
warcToSpark.jar input_folder output_folder


NB! When working with HDFS, input files have to be moved not copied!

Suggested behaviour is to first copy input files to a temporary folder and then move them to the input folder of the streaming job:

hadoop dfs -copyFromLocal /mnt/warcs/warc1.warc inputToCopy

hadoop dfs -mv inputToCopy/* input

