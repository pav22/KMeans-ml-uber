# KMeans-ml-uber

# EmailSpamFilter

To run this code, follow below steps:

1) clone or download this code.
2) Import this project into Scala IDE.
3) Copy data present in Data folder to /data/uberdata/uberApr14.txt path in hadoop box.
4) Mention this same path in UberAnalysis.scala file.
5) Right click on project and "Run As" --> "Maven Install"
6) After jar is created, using WinSCP copy the jar file to cloudera or other Hadoop distribution box.
7) Navigate to jar path then, use below command in shell.

spark-submit  --master yarn-client --driver-memory 512m --executor-memory 512m  --class  kmeans.UberAnalysis  KmeansUber-0.0.1-SNAPSHOT-jar-with-dependencies.jar