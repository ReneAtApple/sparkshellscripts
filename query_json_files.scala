val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val df = sqlContext.read.json("/hduser/testdata/people.json")

sc.setJobGroup("2", "TRP:Query on JSON file")

//import sqlContext.implicits._

df.printSchema()
// root
 // |-- _corrupt_record: string (nullable = true)
 // |-- city: string (nullable = true)
 // |-- creditcard: string (nullable = true)
 // |-- email: string (nullable = true)
 // |-- mac: string (nullable = true)
 // |-- name: string (nullable = true)
 // |-- timestamp: string (nullable = true)



df.select(df("name"), df("*")).show()
// +--------------------+---------------+--------------------+-------------------+--------------------+-----------------+--------------------+--------------------+
// |                name|_corrupt_record|                city|         creditcard|               email|              mac|                name|           timestamp|
// +--------------------+---------------+--------------------+-------------------+--------------------+-----------------+--------------------+--------------------+
// |                null|              [|                null|               null|                null|             null|                null|                null|
// |        Keeley Bosco|           null|     Lake Gladysberg|1228-1221-1221-1431|katlyn@jenkinsmag...|08:fd:0b:cd:77:f7|        Keeley Bosco|2015-04-25 13:57:...|
// |         Rubye Jerde|           null|                null|1228-1221-1221-1431|juvenal@johnston....|90:4d:fa:42:63:a2|         Rubye Jerde|2015-04-25 09:02:...|
// |Miss Darian Breit...|           null|                null|               null|                null|f9:0e:d3:40:cb:e9|Miss Darian Breit...|2015-04-25 13:16:...|
// |    Celine Ankunding|           null|                null|1228-1221-1221-1431|emery_kunze@rogah...|3a:af:c9:0b:5c:08|    Celine Ankunding|2015-04-25 14:22:...|
// |    Dr. Araceli Lang|           null|         Yvettemouth|1211-1221-1234-2201|mavis_lehner@jaco...|9e:ea:28:41:2a:50|    Dr. Araceli Lang|2015-04-25 21:02:...|
// |         Esteban Von|           null|                null|               null|                null|2d:e4:f0:dd:90:96|         Esteban Von|2015-04-25 21:47:...|
// |      Everette Swift|           null|                null|               null|gielle_jacobs@fla...|29:e0:54:7a:b7:ca|      Everette Swift|2015-04-25 01:42:...|
// |       Terrell Boyle|           null|     Port Reaganfort|1228-1221-1221-1431|augustine.conroy@...|c5:32:09:5a:f7:15|       Terrell Boyle|2015-04-25 23:03:...|
// |   Miss Emmie Muller|           null|          Kaleyhaven|               null|                null|be:dc:d2:57:81:8b|   Miss Emmie Muller|2015-04-25 15:48:...|
// |        Libby Renner|           null|      Port Reneeside|1234-2121-1221-1211|                null|9c:63:13:31:c4:ac|        Libby Renner|2015-04-25 08:21:...|
// |        Kris Spencer|           null|                null|               null|                null|f9:8a:01:69:aa:63|        Kris Spencer|2015-04-25 18:49:...|
// |   Terrance Schiller|           null|                null|               null|prince@rosenbaum....|fd:b7:2a:2e:97:8f|   Terrance Schiller|2015-04-25 02:25:...|
// |   Alessandro Barton|           null|         South Pearl|1234-2121-1221-1211|sigurd.hudson@hod...|1e:44:17:8c:c6:d8|   Alessandro Barton|2015-04-25 11:21:...|
// |      Dr. Art Grimes|           null|                null|1211-1221-1234-2201|   monica@abbott.org|bf:2a:a2:75:a4:38|      Dr. Art Grimes|2015-04-25 22:51:...|
// |         Keven Purdy|           null|Port Marjolaineshire|1211-1221-1234-2201|carter_zboncak@sc...|cd:a7:57:c0:03:50|         Keven Purdy|2015-04-25 10:13:...|
// |  William Wisozk DVM|           null|          Rogelioton|               null|     adonis@dach.net|9a:6e:08:fe:8d:41|  William Wisozk DVM|2015-04-25 20:57:...|
// |           Oma Grady|           null|       South Chelsie|1212-1221-1121-1234|laurianne_goldner...|9e:60:32:9f:88:9d|           Oma Grady|2015-04-25 04:08:...|
// |   Annie Schamberger|           null|         Marvinville|               null|blanca_smitham@pf...|f0:45:37:1b:d8:19|   Annie Schamberger|2015-04-25 10:28:...|
// |      Jazmin Kovacek|           null|                null|1211-1221-1234-2201|mittie.ullrich@bo...|13:36:6a:b4:2d:07|      Jazmin Kovacek|2015-04-25 19:20:...|
// +--------------------+---------------+--------------------+-------------------+--------------------+-----------------+--------------------+--------------------+

df.filter($"name".contains("urdy"))
df.filter($"name".contains("urdy")).show()
df.select(df("name"), df("*")).show()
df.select(df("name")).show()
df.select(df("*")).show()
df.filter($"name".contains("urdy")).filter($"name".contains("ayra")).show()
