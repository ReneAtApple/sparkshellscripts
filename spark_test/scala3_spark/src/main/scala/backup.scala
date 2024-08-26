import scala.Predef._          
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession};
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.functions._ 

// val rddFromFile = sc.textFile("hdfs://localhost:54310/hduser/testdata/bankdata/txt/Rekeningafschrift_Seqnr20220009.txt")
// session.read.load("file:///home/hduser/projects/scala/workspace/sparkshellscripts/spark_test/scala3_spark/src/main/resource/cust-contact.csv") // OK
// session.read.load("hdfs://localhost:54310/hduser/testdata/currying_example_data/cust-contact.csv") // OK
val root_path = "file:///home/hduser/projects/scala/workspace/sparkshellscripts/spark_test/scala3_spark/src/main/resource/"
val session = SparkSession.builder().appName(name = "test").master(master = "local").getOrCreate()


// val custContactSchema = StructType(
    // List (
        // StructField("customer_id",StringType, true),
        // StructField("customer_name",StringType, true),
        // StructField("customer_contact", StringType, true)
    // )
// )

// val custContactDF = session.read
      // .schema(custContactSchema)
      // .option("delimiter",";")
      // .option("header", true)
      // .csv( path = "cust-contact.csv")

// val custImpactSchema = StructType(
    // List (
        // StructField("customer_id",StringType, true),
        // StructField("product_id",StringType, true),
        // StructField("pr_ct", StringType, true),
        // StructField("time_span", StringType, true)
    // )
// )

// val custImpressionsDF = session.read
    // .schema(custImpactSchema)
    // .option("inferSchema", true) 
    // .option("delimiter", ";")
    // .option("header", true)    
    // .csv( path = root_path + "cust-contact.csv")

def joinOnKey(df1: DataFrame)(joinKey: String)(df2: DataFrame): DataFrame = {
        df2.join(df1, Seq(joinKey), joinType = "Left")
    }

def joinOnKey2(df1: DataFrame, joinKey: String, df2: DataFrame): DataFrame = {
        df2.join(df1, Seq(joinKey), joinType = "Left")
    }

val custContactDFvs2    = session.read.format("csv").load(root_path + "cust-contact.csv")
val custImpressionsDFv2 = session.read.format("csv").load(root_path + "cust-impact.csv")
val prdIDDF             = session.read.format("csv").load(root_path + "prd-id.csv")

def getCustomerContact:DataFrame=> DataFrame = joinOnKey(custContactDFvs2)(joinKey = "_c0")(_)
def getCustomerContactCurried: DataFrame => String => DataFrame => DataFrame = (joinOnKey2 _).curried
def c1: String => DataFrame => DataFrame = getCustomerContactCurried(custImpressionsDFv2)

val getProductInfo:DataFrame = joinOnKey(custImpressionsDFv2)(joinKey = "_c0")(prdIDDF)

getCustomerContact(custImpressionsDFv2).show( truncate = false)

getProductInfo.show( truncate = false)



