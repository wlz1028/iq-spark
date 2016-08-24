package ca.infoq.spark.common.config

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by infoq.ca(Edward,Wang) on 2016-08-20.
  */
object App {
  def main(args: Array[String]): Unit = {
    println("start")
    val conf = new SparkConf().setMaster("local[*]").setAppName("test").set("spark.driver.memory", "1g")
    val _sc = new SparkContext(conf)
    val _sqlContext = new org.apache.spark.sql.hive.HiveContext(_sc)
    val _log = Logger.getLogger(getClass.getName)
    val data = Array(1, 2, 3, 4, 5)
    val distData = _sc.parallelize(data)
    println(distData.count())
  }

}
