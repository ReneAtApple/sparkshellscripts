import scala.Predef._          
import org.apache.spark.SparkContext._ 
import sqlContext.implicits._  
import sqlContext.sql          
import org.apache.spark.sql.functions._ 

@main
object WordCount {
    def main(args: Array[String]) {
  
  
  
        // /* configure spark application */
        // val conf = new SparkConf().setAppName("Spark Scala WordCount Example").setMaster("local[1]")

        // /* spark context*/
        // val sc = new SparkContext(conf)
  
        // /* map */
        // var map = sc.textFile("input.txt").flatMap(line => line.split(" ")).map(word => (word,1))
  
        // /* reduce */
        // var counts = map.reduceByKey(_ + _)
  
        // /* print */
        // counts.collect().foreach(println)
  
        // /* or save the output to file */
        // counts.saveAsTextFile("out.txt")
  
        sc.stop()
    }
}


