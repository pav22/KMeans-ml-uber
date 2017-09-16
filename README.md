# KMeans-ml-uber

# EmailSpamFilter

To run this code, follow below steps:

1) clone or download this code.
2) Import this project into Scala IDE.
3) Copy data present in Data folder to /data/uberdata/uberApr14.txt path in hadoop box.
4) Right click on project and "Run As" --> "Maven Install"
5) After jar is created, copy  to  cloudera or other Hadoop distribution box.
6) Navigate to jar path then, use below command in shell.

spark-submit  --master yarn-client --driver-memory 512m --executor-memory 512m  --class  sparkml.EmailFilter  EmailSpamFilter-0.0.1-SNAPSHOT-jar-with-dependencies.jar