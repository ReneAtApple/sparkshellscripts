import org.apache.spark.sql.hive.HiveContext	// hive context
val sqlContext = new HiveContext(sc) 			// Make sure you use HiveContext
import sqlContext.implicits._ 					// Optional, just to be able to use toDF

val df = Seq(("foo", "bar"), ("foobar", "foo"), ("foobar", "bar")).toDF("a", "b")

sc.setJobGroup("2", "TRP:query on hive table")

df.registerTempTable("mytable")
df.printSchema()
df.select(df("*")).show()
sqlContext.sql("SELECT * FROM mytable WHERE a LIKE CONCAT('%ar%')").show()

