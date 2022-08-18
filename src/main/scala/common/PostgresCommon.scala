package common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

object PostgresCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def getPostgresCommonProps() : Properties = {
    //Create a dataframe from Postgres Course Catalog table
    logger.info("getPostgresCommonProps method started")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user", "postgres")
    pgConnectionProperties.put("password", "amit27")
    logger.info("getPostgresCommonProps method ended")
    pgConnectionProperties
  }

  def getPostgresServerDatabase() : String = {
    val pgURL = "jdbc:postgresql://localhost:5432/futurex"
    pgURL
  }

  def fetchDataframeFromPgTable(spark : SparkSession,pgTable : String) : Option[DataFrame] = {
    try{
      logger.info("fetchDataframeFromPgTable method started")
      //server:port/database_name
      val pgCourseDataframe = spark.read.jdbc(getPostgresServerDatabase(), pgTable, getPostgresCommonProps())
      logger.info("fetchDataframeFromPgTable method ended")
      Some(pgCourseDataframe)
    } catch {
      case e: Exception =>
        logger.error("An error has occured in fetchDataframeFromPgTable method " + e.printStackTrace())
        System.exit(1)
        None

    }
  }

  def writeDFToPostgresTable(dataFrame: DataFrame, pgTable: String): Unit = {
    try {
      logger.warn("writeDFToPostgresTable method started")

      dataFrame.write
        .mode(SaveMode.Append)
        .format("jdbc")
        .option("url", getPostgresServerDatabase())
        .option("dbtable", pgTable)
        .option("user", "postgres")
        .option("password", "amit27")
        .save()
      logger.warn("writeDFToPostgresTable method ended")

    } catch {
      case e: Exception =>
        logger.error("An error occured in writeDFToPostgresTable " + e.printStackTrace())
    }
  }

}
