import org.apache.hadoop.fs.{FileSystem,Path}
// this whole block can be pasted in spark-shell in :paste mode followed by <Ctrl>D
sc.setJobGroup("2", "TRP: Sort By key")

val file = sc.textFile("/hduser/testdata/some_data.txt")
val wordCounts = file.flatMap(line => line.split(" "))
.map(word => (word, 1))
.reduceByKey(_ + _, 1)  // 2nd arg configures one task (same as number of partitions)
.map(item => item.swap) // interchanges position of entries in each tuple
.sortByKey(true, 1) // 1st arg configures ascending sort, 2nd arg configures one task
.map(item => item.swap)

val fs = FileSystem.get(sc.hadoopConfiguration)
val outPutPath = new Path("/hduser/testdata/WordCountscala.out")
if (fs.exists(outPutPath)) fs.delete(outPutPath, true)

wordCounts.collect
wordCounts.saveAsTextFile("/hduser/testdata/WordCountscala.out")
