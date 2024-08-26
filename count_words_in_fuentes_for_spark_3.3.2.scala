// https://stackoverflow.com/questions/53497142/how-can-count-words-of-multiple-files-present-in-a-directory-using-spark-scala
// https://sparkbyexamples.com/pyspark/pyspark-rdd-transformations/

import org.apache.hadoop.fs.{FileSystem, Path}

val outputPath = "/hduser/testdata/fuentes/output"
val filesContent = sc.wholeTextFiles("/hduser/testdata/fuentes/*.txt")
val fs = FileSystem.get(sc.hadoopConfiguration)
fs.delete(new Path(outputPath), true)
// Remove the output directory if it exists
val words = filesContent.flatMap { case (_, fileContent) =>
  fileContent.toLowerCase.replaceAll("[\r\n.,/:_?)(]|[0-9]", " ").split(" ")
}
val wordCountRdd = words.map(word => (word, 1))
val wordCounts = wordCountRdd.reduceByKey(_ + _)

val sortedWordCounts = wordCounts.sortBy( _._2, ascending = true)
// val sortedWordCounts = wordCounts.sortBy(-_._2)  // sort on the second element (counts) in descending order

sortedWordCounts.saveAsTextFile("/hduser/testdata/fuentes/output/dump.txt")
// sortedWordCounts.foreach(println)
