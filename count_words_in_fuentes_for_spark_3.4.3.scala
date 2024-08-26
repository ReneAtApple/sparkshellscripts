import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

// Create a Hadoop configuration
val conf = new Configuration()
conf.set("fs.defaultFS", "hdfs://localhost:9000")

// Get the HDFS FileSystem
val fs = FileSystem.get(conf)

// Define the output path
val outputPath = new Path("hdfs://localhost:9000/foreign_data/imported/fuentes/output")
// Check if the output directory exists
if (fs.exists(outputPath)) {
    // Attempt to delete the output directory
    val deleted = fs.delete(outputPath, true) // true for recursive delete
    if (deleted) {
        println(s"Deleted output path: $outputPath")
    } else {
        println(s"Failed to delete output path: $outputPath")
    }
} else {
    println(s"Output path does not exist: $outputPath")
}

val filesContent = sc.wholeTextFiles("hdfs://localhost:9000/foreign_data/imported/fuentes/*.txt")
val words = filesContent.flatMap { case (_, fileContent) =>
  fileContent.toLowerCase.replaceAll("[\r\n.,/:_?)(]|[0-9]", " ").split(" ")
}
val wordCountRdd = words.map(word => (word, 1))
val wordCounts = wordCountRdd.reduceByKey(_ + _)

// val sortedWordCounts = wordCounts.sortBy( _._2, ascending = true)
val sortedWordCounts = wordCounts.sortBy(-_._2)  // sort on the second element (counts) in descending order

sortedWordCounts.saveAsTextFile("hdfs://localhost:9000/foreign_data/imported/fuentes/dump.txt")
sortedWordCounts.foreach(println)
