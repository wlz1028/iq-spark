package ca.infoq.spark.app.hivesql

import ca.infoq.spark.common.config.{IqApp, IqAppArgsParser}
import ca.infoq.spark.common.sql.IqSqlUtils.runSql
import ca.infoq.spark.common.io.IoUtils.getFileStrFromResource
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by infoq.ca(Edward,Wang) on 2016-08-22.
  */
object IqSqlApp {
  def main(args: Array[String]) {
    val confMap = IqAppArgsParser.getArgsMap(args)
    /*    val app = new IqApp(confMap)

        //scala implicit explained: http://stackoverflow.com/questions/10375633/understanding-implicit-in-scala
        implicit val sc = app._sc
        implicit val sqlContext = app._sqlContext*/

    val conf = new SparkConf().setMaster("local[*]").setAppName("IqSqlApp")
    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    val transFile = "src/main/resources/data/fakeTransaction.csv"

    //Load sample data
    sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .load(transFile)
      .registerTempTable("trans")

    //rename method to a shorter name
    def getSqlStr = getFileStrFromResource

    //run sql with pre-defined vars and then registerTempTable
    runSql(getSqlStr("sql/q1_cust_montly_sum.sql"), confMap, "r1")
    sqlContext.table("r1").show()

    //run another sql from previous result
    runSql(getSqlStr("sql/q2_select_by_id.sql"), confMap, "r2")
    sqlContext.table("r2").show()

    //Another demo on sql variableSubstitution which similar to hiveconf/hivevar
    runSql(getSqlStr("sql/q3_filter_by_month.sql"), confMap, "r3")
    sqlContext.table("r3").show()
  }
}
