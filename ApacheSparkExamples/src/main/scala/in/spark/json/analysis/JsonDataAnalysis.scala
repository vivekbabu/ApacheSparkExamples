package in.spark.json.analysis

import org.apache.spark.SparkContext

object JsonDataAnalysis {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "JSON Analysis")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val zips = sqlContext.jsonFile("hdfs://quickstart.cloudera:8020/user/cloudera/zips/zips.json");
    zips.printSchema
  }

}