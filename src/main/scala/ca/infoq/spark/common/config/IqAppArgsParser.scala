package ca.infoq.spark.common.config

/**
  * Created by infoq.ca(Wang,Edward) on 8/20/16.
  */
object IqAppArgsParser {
  val regex = new scala.util.matching.Regex("""(\S+)=\b(\S+)\b""")

  def getArgsMap(args: Array[String]): Map[String, String] = {
    val argStr = args.mkString(" ")
    getArgsMap(argStr)
  }

  def getArgsMap(argStr: String): Map[String, String] = {
    (for (regex(k, v) <- regex findAllIn argStr) yield (k, v)).toMap
  }
}
