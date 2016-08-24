package ca.infoq.spark.common.config

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by infoq.ca(Wang,Edward) on 8/20/16.
  */
class IqApp(confMap: Map[String, String]) extends IqConfig(confMap){
  val conf = new SparkConf().setAppName(getClass.getSimpleName)
  val _sc = new SparkContext(conf)
  //Since my machine doesn't have hive installed, I will use SQLContext instead
  //val _sqlContext = new org.apache.spark.sql.hive.HiveContext(_sc)
  val _sqlContext = new org.apache.spark.sql.SQLContext(_sc)
  val _log = Logger.getLogger(getClass.getName)
}
