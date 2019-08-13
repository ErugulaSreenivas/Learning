RDD is a distributed collection of elements across the nodes in cluster. Also we can performs parallel operations. Moreover, Spark RDDs are immutable in nature. Although, we can create new RDD by transforming existing Spark RDD

change the file
**************************************************************


****************************************************

The internal concept of RDD is, it will distribute the data across nodes in the cluster, also we can performs parallel operations with RDD.
basically spark RDDs are immutable nature, we can create new RDD by transforming existing RDD.

>>> data = sc.parallelize([('Amber', 22), ('Alfred', 23), ('Skye',4), ('Alfred', 23), ('Skye',4), ('Albert', 12), ('Vivek', 9),('Slobo', 22), ('Alfred', 23), ('Skye',4), ('Joan', 12), ('Rayan', 9)])

>>> data.collect()
[('Amber', 22), ('Alfred', 23), ('Skye', 4), ('Albert', 12), ('Amber', 9)]
>>> data.first()
('Amber', 22)
>>> data.count()
5

if we want to run pysaprk script we need to use below command:
#spark-submit word_count.py

text_file = sc.textFile("/user/sreeni/word_count.txt")
counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("/user/sreeni/word_count_output1")

spark is one of the hadoop framework
we can analyze batch data as well as
realtime data with the help of spark
----------------------------------------------------------------------------------------------------------------------------------------
#word count in pyspark:
file = sc.textFile("/user/sreeni/word_count.txt")
counts = file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("/user/sreeni/wordcount_output_latest_dhvani")
------------------------------------------------------------------------------------------------------------------------------------------
file name: word_count.py

from pyspark import SparkContext
sc =SparkContext()
file = sc.textFile("/user/sreeni/word_count.txt")
counts = file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("/user/sreeni/wordcount_output_latest")

execute command: spark-submit word_count.py
-----------------------------------------------------------------------------------------------
programming in scala:-
val distFile = sc.textFile("/user/sreeni/word_count.txt")

val lines = sc.textFile("/user/sreeni/word_count.txt")
val lineLengths = lines.map(s => s.length)
val totalLength = lineLengths.reduce((a, b) => a + b)
lineLengths.persist()

val lines = sc.textFile("/user/sreeni/word_count.txt")
val pairs = lines.map(s => (s, 1))
val counts = pairs.reduceByKey((a, b) => a + b)

scala> val data = Array(1, 2, 3, 4, 5)
data: Array[Int] = Array(1, 2, 3, 4, 5)

scala> val distData = sc.parallelize(data)
distData: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:23

scala> val distFile = sc.textFile("/user/sreeni/word_count.txt")
distFile: org.apache.spark.rdd.RDD[String] = /user/sreeni/word_count.txt MapPartitionsRDD[2] at textFile at <console>:21

scala> distFile.collect()
res0: Array[String] = Array(spark is one of the hadoop framework, we can analyze batch data as well as, realtime data with the help of spark)

scala> distFile.count()
res1: Long = 3

scala> distFile.first()
res2: String = spark is one of the hadoop framework

scala> val lines = sc.textFile("/user/sreeni/word_count.txt")
lines: org.apache.spark.rdd.RDD[String] = /user/sreeni/word_count.txt MapPartitionsRDD[4] at textFile at <console>:21

scala> val lineLengths = lines.map(s => s.length)
lineLengths: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[5] at map at <console>:23

scala> val totalLength = lineLengths.reduce((a, b) => a + b)
totalLength: Int = 108

scala> lineLengths.persist()
res3: lineLengths.type = MapPartitionsRDD[5] at map at <console>:23

scala> val lines = sc.textFile("/user/sreeni/word_count.txt")
lines: org.apache.spark.rdd.RDD[String] = /user/sreeni/word_count.txt MapPartitionsRDD[7] at textFile at <console>:21

scala> val pairs = lines.map(s => (s, 1))
pairs: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[8] at map at <console>:23

scala> val counts = pairs.reduceByKey((a, b) => a + b)
counts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[9] at reduceByKey at <console>:25

scala> counts.collect()
res4: Array[(String, Int)] = Array((spark is one of the hadoop framework,1), (we can analyze batch data as well as,1), (realtime data with the help of spark,1))

scala> counts.count()
res5: Long = 3

scala>
