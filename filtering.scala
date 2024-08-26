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
val textFile = sc.textFile("/hduser/testdata/some_data.txt")
// val textFile = sc.textFile("/hduser/testdata/input.sql.big")
sc.setJobGroup("14", "TRP:Count prd in sql scripts")
textFile.first()
val linesWithSpark = textFile.filter(line => line.contains(""))
// val linesWithSpark = textFile.filter(line => line.contains("PRD"))
linesWithSpark.count()
System.out.println(linesWithSpark.count());
linesWithSpark.saveAsTextFile("/hduser/testdata/some_data.out")

// val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
// fs.removeAcl(new Path("/hduser/testdata/spark_job.out"))


// s"hdfs dfs -rm -r /hduser/testdata/spark_job.out"
// s"hdfs dfs -rm    /hduser/testdata/spark_job.out"

var strResult: String = "Glazed Donut"
// String strResult = linesWithSpark.count().toString(date, null);
// String strResult = linesWithSpark.count().toString();

val num = linesWithSpark.count()
val strnum = num.toString()
System.out.println(strnum);


import java.io._
// val file = new File("/hduser/testdata/spark_job.out")
// val bw = new BufferedWriter(new FileWriter(file))

linesWithSpark.collect().foreach(println)

linesWithSpark.count().toString().saveAsTextFile("/hduser/testdata/spark_job.out")





val text = sc.textFile("/hduser/testdata/input.sql.big")
val counts = text.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
counts.collect

