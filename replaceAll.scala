import org.apache.hadoop.fs.{FileSystem,Path}

val fs = FileSystem.get(sc.hadoopConfiguration)
val outPutPath = new Path("/hduser/testdata/WordCountscala.out")
if (fs.exists(outPutPath)) fs.delete(outPutPath, true)

sc.setJobGroup("2", "TRP:Count each word")
val reg = """[\t\p{Zs}]+"""
val text = sc.textFile("/hduser/testdata/input.sql.big")
val counts = text.flatMap(
	    line => line.trim.replaceAll("[\t',()-+=<>]", " ").replaceAll(" +", " ").split(" ")
	).map(
		word => (word,1)
	).reduceByKey(_+_)

counts.collect
counts.saveAsTextFile("/hduser/testdata/WordCountscala.out")

$intp.definedTerms


// exit()

// rdd_nopunc = rdd.flatMap(
		// lambda x: x.split()
	// ).filter(
		// lambda x: x.replace("[,.!?:;]", "")
	// )


// val s = "ABCD        A M@L                             79\nBGDA        F D@L                             89"
// val reg = """[\t\p{Zs}]+"""
// val cleanedRDD2 = s.replaceAll(reg, ",")
// print(cleanedRDD2)


// .trim.replaceAll(" +", " ")

// .filter(line => line.contains("DWH_PRD"))

