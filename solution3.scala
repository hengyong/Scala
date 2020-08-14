import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, desc}

object solution3 {
	
	case class apat6399(
		patent:String,
		gyear:String,
		gdate:String,
		appyear:String,
		country:String
		)
	case class cite7599(
		citing:String,
		cited:String
		)


	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("task3")
			.config("spark.master", "local")
			.config("hive.metastore.uris", "thrift://localhost:9083")
			.config("hive.metastore.schema.verification", "true")
			.enableHiveSupport()
			.getOrCreate()

		spark.sparkContext.setLogLevel("ERROR")
		
		
		import spark.implicits._
		val PatDF = spark.sparkContext.textFile("/user/bigdata/apat63_99.txt").map(_.split(",")).map(attributes => solution3.apat6399(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4) )).toDF()
		PatDF.createOrReplaceTempView("patenttable")

		import spark.sql
		

		import org.apache.spark.sql.{SaveMode}
		val citedDF = spark.sparkContext.textFile("/user/bigdata/cite75_99.txt").map(_.split(",")).map(attributes => solution3.cite7599(attributes(0), attributes(1) )).toDF()
		citedDF.createOrReplaceTempView("citedtable")
		val filterAus = sql("select * from patenttable where country like '%AU%'").limit(20).show()
		val resultDF = sql("select cited, count(*) citedCount from patenttable, citedtable where patenttable.country like '%AU%' and citedtable.cited = patenttable.patent group by cited order by count(*) desc").limit(20)

		resultDF.write.mode(SaveMode.Overwrite).saveAsTable("HiTab")
		spark.stop()
	}
}
