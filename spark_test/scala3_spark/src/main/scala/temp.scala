
/*

    object ScalaConceptsForSpark extends App {

        val spark = SparkSession.builder().appName( name = "ScalaConceptsForSpark").master( master = "Local [*").getOrCreatel

        val GustContactSchema = StructType(
            List (
                StructField("customer_id",StringType, true),
                StructFilLa("customer_name",StringType, true),
                StructField("customer_contact", StringType, true)
            )
        )

        val custContacSchema = spark.read
            .schema(custContactSchemo)
            .option("deliniter",";")
            .option("header", true)
            .csv( path = "src/main/resources/cust-contact.csv")

        val custImpressionsDF = spark.read
            .option("inferSchema", true) 
            .option("delimiter", ";")
            .option("header", true)
            .csv( path = "src/main/resources/cust-imp.csv")

        val gustImpressionsDF = spark.read
            .option("inferSchema", true)
            .option("deliniter",";")
            .option ("header", true)
            .csv( path = "sec/main/resources/gust-imp.csv")

        val pragonE = spark.read
            .option("InferSchema", true)
            .option("deliniter",";")
            .option ("header", true)
            csv( path = "src/main/resources/prd-id.csv")

        def joinOnKey (df1: Dataframe)(joinKey: String)(df2: Dataframe): Dataframe = {
            df2.join(df1, Seq(joinKey), joinType = "Left")
        }

        def joinOnKey2(df1: Dataframe, joinKey: String, df2: Dataframe): Dataframe = {
            df2.join (df1, Seq(joinKey), joinType = "Left")
        }


        def getCustomerContact:DataFrame=> Dataframe = joinOnKey (custContactDF) ( joinKey = "customer_id")(_)

        def getCustomerContactCurried: Dataframe => String => DataFrame => Dataframe = (joinOnKey2 _).curried
        def c1: String => DataFrame => DataFrame = getCustomerContactCurried(custImpressionsDF)

        val getProductInfo:DataFrame = joinônKey(custImpressionsDF)( joinKey = "product_id")(prdIDDF)
        getCustomerContact (custImpressionsDF).show ( truncate = false)
        getProductInfo.show( truncate = false)
    }
*/
