package ca.infoq.spark.common.sql

import org.apache.spark.sql.SQLContext

/**
  * Created by infoq.ca(Wang,Edward) on 8/20/16.
  */
object IqSqlUtils {
  def resolveDollarVar(sqlStr: String, vars: Map[String, String]): String = {
    val varPattern = new scala.util.matching.Regex("""(\$\{(\S+?)\})""", "fullVar", "value")

    //Resolve/replace $vars from vars: Map[String, String]
    varPattern replaceAllIn (sqlStr, m => {
      try {
        vars(m.group("value"))
      } catch {
        case e: NoSuchElementException => throw new NoSuchElementException("Error: " + m.group("fullVar") + " cannot be resolved")
      }
    })
    //TODO: check if all $ variables are resolved or not
  }

  def runSql(sqlStr: String, vars: Map[String, String], outTableName: String, persistFlg: Boolean = false)(implicit sqlContext: SQLContext): Unit = {
    sqlContext.sql(resolveDollarVar(sqlStr, vars)).registerTempTable(outTableName)
    if (persistFlg) sqlContext.cacheTable(outTableName)
  }

}
