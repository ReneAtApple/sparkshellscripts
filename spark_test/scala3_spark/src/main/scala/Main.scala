import scala.Predef._          
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, SparkSession};
// import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.functions._ 

val root_path = "file:///home/hduser/projects/scala/workspace/sparkshellscripts/spark_test/scala3_spark/src/main/resource/"
val session = SparkSession.builder().appName(name = "test").master(master = "local").getOrCreate()

def joinOnKey(df1: DataFrame)(joinKey: String)(df2: DataFrame): DataFrame = {
        df2.join(df1, Seq(joinKey), joinType = "Left")
    }

def joinOnKey2(df1: DataFrame, joinKey: String, df2: DataFrame): DataFrame = {
        df2.join(df1, Seq(joinKey), joinType = "Left")
    }

val custContactDDF     = session.read.format("csv").option("sep",";").option("header", "true").load(root_path + "cust-contact.csv")
val custImpressionsDDF = session.read.format("csv").option("sep",";").option("header", "true").load(root_path + "cust-impact.csv")
val productDDF         = session.read.format("csv").option("sep",";").option("header", "true").load(root_path + "prd-id.csv")

def getCustomerContact           : DataFrame => DataFrame                            = joinOnKey(custContactDDF)(joinKey = "customer_id")(_)
// def getCustomerContactCurried : DataFrame => String     => DataFrame => DataFrame = (joinOnKey2 _).curried
// def c1                        : String    => DataFrame  => DataFrame              = getCustomerContactCurried(custImpressionsDDF)

val getProductInfo:DataFrame = joinOnKey(custImpressionsDDF)(joinKey = "product_id")(productDDF)

// getCustomerContact(custImpressionsDDF).show( truncate = false)
// getProductInfo.show( truncate = false)

// def getCustomerImpactProduct     : DataFrame => DataFrame                            = joinOnKey(custImpressionsDDF)(joinKey = "product_id")(_)
// getCustomerImpactProduct(getProductInfo).show( truncate = false)

// val getAllInfo:DataFrame = joinOnKey(custImpressionsDDF)(joinKey = "product_id")(getCustomerContact(custImpressionsDDF))
// getAllInfo.show( truncate = false)

joinOnKey(session.read.format("csv").option("sep",";").option("header", "true").load(root_path + "cust-contact.csv"))(joinKey = "customer_id")(session.read.format("csv").option("sep",";").option("header", "true").load(root_path + "cust-impact.csv")).show( truncate = false)
