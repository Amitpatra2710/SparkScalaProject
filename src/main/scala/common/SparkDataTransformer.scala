package common

import org.apache.log4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame

object SparkDataTransformer {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def replaceNullValues(dataFrame : DataFrame) : DataFrame = {
    logger.warn("replaceNullValues method started")
    val author_name_value: String = FXJsonParser.returnConfigValue("body." +
      "replace_null_value.author_name")
    logger.warn("author_name_value " + author_name_value)
    val no_of_reviews_value: String = FXJsonParser.returnConfigValue("body." +
      "replace_null_value.no_of_reviews")
    logger.warn("no_of_reviews_value " + no_of_reviews_value)

    if ((author_name_value == "YES") &&
      (no_of_reviews_value == "YES")) {
      logger.warn("Both YES ")
      val transformedDF = dataFrame.na.fill("Unknown", Seq("author_name"))
        .na.fill(value = "0", Seq("no_of_reviews"))
      transformedDF
    } else if ((author_name_value == "YES") &&
      (no_of_reviews_value == "NO")) {
      logger.warn("Only Author Name Yes ")
      val transformedDF = dataFrame.na.fill("Unknown", Seq("author_name"))
      transformedDF
    } else if ((author_name_value == "NO") &&
      (no_of_reviews_value == "YES")) {
      logger.warn("Only Number of Review Yes ")
      val transformedDF = dataFrame.na.fill(value = "0", Seq("no_of_reviews"))
      transformedDF
    } else {
      dataFrame
    }

  }

}
