package ca.infoq.spark.common.config

/**
  * Created by infoq.ca(Wang,Edward) on 8/20/16.
  */
class IqConfig(confMap: Map[String, String]) {
  /*  Define required constant names in your app
    Feel free to add more params if you need more control */
  val _ENV_NAME: String = "ENV_NAME"
  val _PROJECT_NAME: String = "PROJECT_NAME"

  val paramList = List(_ENV_NAME, _PROJECT_NAME)

  val paramsMap =
    paramList.map(k => confMap.get(k) match {
      case Some(v) => (k, v)
      case _ => throw new NoSuchElementException(s"Config param ${k} is not find in conf")
    }).toMap

  val ENV_NAME: String = paramsMap.get(_ENV_NAME).get
  val PROJECT_NAME: String = paramsMap.get(_PROJECT_NAME).get

  def getOutDataDir: String = getOutEnvRoot + "/data"

  def getOutEnvRoot: String = s"/workspace/spark/${PROJECT_NAME}/${ENV_NAME}".toLowerCase()

  def getOutDatabaseName: String = s"${PROJECT_NAME}_${ENV_NAME}".toUpperCase()
}
