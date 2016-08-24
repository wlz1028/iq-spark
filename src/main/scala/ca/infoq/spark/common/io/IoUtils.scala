package ca.infoq.spark.common.io

/**
  * Created by infoq.ca(Edward,Wang) on 2016-08-22.
  */
object IoUtils {
  //Use relative path from resource(e.g. data/fakeTransaction.csv)
  val getFileStrFromResource = (path: String) => scala.io.Source.fromInputStream(this.getClass.getClassLoader.getResourceAsStream(path)).mkString
}
