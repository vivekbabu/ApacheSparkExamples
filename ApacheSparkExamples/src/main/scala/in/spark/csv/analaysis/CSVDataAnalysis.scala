package in.spark.csv.analaysis

import org.apache.spark.SparkContext

object CSVDataAnalysis {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "CSV Analysis")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val clickStream = sqlContext.load("com.databricks.spark.csv", Map("path" -> "hdfs://quickstart.cloudera:8020/user/cloudera/clickstreamcsv/clickstream.log.csv", "header" -> "true"))
    clickStream.printSchema
    clickStream.registerTempTable("clickstream")
    val engineeringManagerApplicationCount = sqlContext.sql("SELECT count(*) FROM clickstream WHERE Page='Careers' AND Button='Engineering Manager'")
   println(engineeringManagerApplicationCount.collect.toList)
   
  }
}