import org.apache.hadoop.fs.{FileSystem,Path}
// FileSystem.get( sc.hadoopConfiguration ).listStatus( new Path("hdfs:///tmp")).foreach( x => println(x.getPath ))
FileSystem.get( sc.hadoopConfiguration ).listStatus( new Path("/hduser/testdata/input.sql")).foreach( x => println(x.getPath ))
path = hadoop.fs.Path('/')
FileSystem.get( sc.hadoopConfiguration ).listStatus( new Path("hdfs:///tmp")).foreach( x => println(x.getPath ))
val textFile = sc.textFile("/hduser/testdata/")
val linesWithSpark = textFile.filter(line => line.contains("Spark"))
val linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark.count()

import java.lang.Math
// import java.lang.Math
val linesWithSpark = textFile.filter(line => line.contains("Spark"))
val textFile = sc.textFile("/usr/local/spark/README.md")
val linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark.count()
val textFile = sc.textFile("README.md8")
val errorCount = sc.textFile("hdfs://*")
.flatMap(_.split(" ")).filter(_ == "error").count
val errorCount = sc.textFile("hdfs://hduser/testdata/*")
val this_count = sc.textFile("hdfs://hduser/testdata/output.sql")
this_count.count()
val textFile = sc.textFile("/hduser/testdata/input.sql")
textFile.first()
val linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark.count()
textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
	
import java.lang.Math
textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey
val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
val textFile = sc.textFile("/usr/local/spark/README.md")

import org.apache.hadoop.fs.{FileSystem,Path}
val textFile = sc.textFile("/hduser/testdata/input.sql")
textFile.first()
val linesWithSpark = textFile.filter(line => line.contains("DWH_PRD"))
linesWithSpark.count()
