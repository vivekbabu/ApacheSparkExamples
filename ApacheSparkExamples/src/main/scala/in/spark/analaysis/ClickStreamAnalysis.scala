package in.spark.analaysis

import org.apache.spark.SparkContext

object ClickStreamAnalysis {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "cache example")
    val clickStream = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/clickstream/clickstream.log");
    val clickTuples = clickStream.map(clickStreamLine => { val values = clickStreamLine.split("\t"); (values(0), values(1), values(2)) });
    clickTuples.cache();
    val pageClicks = clickTuples.map(tuple => (tuple._1, 1)).reduceByKey(_ + _).collect();
    val engineeringManagerApplicationCount = clickTuples.filter(tuple => tuple._1.equals("Careers") && tuple._2.equals("Engineering Manager")).count();
    val liveChatUseCount = clickTuples.filter(tuple => tuple._1.equals("Support") && tuple._2.equals("Live Chat")).count();
    val guestUseCount = clickTuples.filter(tuple => tuple._3.equals("None")).count()
    println("Page Clicks Are : " + pageClicks.toList)
    println("Number of Engineering Manager Applications Are : " + engineeringManagerApplicationCount)
    println("Number of Live Chat users Are : " + liveChatUseCount)
    println("Number of guest users Are : " + guestUseCount)
    
    
  }

}