package common

import javassist.bytecode.stackmap.BasicBlock.Catch
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object SparkCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def createSparkSession(inputConfig : InputConfig): Option[SparkSession] = {
  try{
  // Create a Spark Session
    // For Windows
    if (inputConfig.env == "dev") {
      logger.info("Setting Hadoop home in a the local environment")
      System.setProperty("hadoop.home.dir", "C:\\winutils")
    }
  logger.info("createSparkSession method started")
  val spark = SparkSession
    .builder
    .appName("HelloSpark")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  logger.info("createSparkSession method ended")

  Some(spark)
  //println("Created Spark Session")
  //val sampleSeq = Seq((1, "spark"), (2, "Big Data"))

  //val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
  //df.show()
  //df.write.format("csv").save("samplesq")
  }catch {
    case e: Exception =>
      logger.error("An error has occured in createSparkSession method " + e.printStackTrace())
      System.exit(1)
      None
  }
  }

  def createCourseHiveTable(spark: SparkSession): Unit = {
    logger.warn("createCourseHiveTable method started")
    spark.sql("create database if not exists fxxcoursedb")
    spark.sql("create table if not exists fxxcoursedb." +
      "fx_course_table(course_id string,course_name string," +
      " author_name string,no_of_reviews string)")
    spark.sql("insert into " +
      "fxxcoursedb.fx_course_table VALUES " +
      "(1,'Java','FutureX',45)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (2,'Java','FutureXSkill',56)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (3,'Big Data','Future',100)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (4,'Linux','Future',100)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (5,'Microservices','Future',100)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (6,'CMS','',100)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (7,'Python','FutureX','')")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (8,'CMS','Future',56)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (9,'Dot Net','FutureXSkill',34)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (10,'Ansible','FutureX',123)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (11,'Jenkins','Future',32)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (12,'Chef','FutureX',121)")
    spark.sql("insert into fxxcoursedb.fx_course_table VALUES (13,'Go Lang','',105)")

    // Treat empty strings as Null
    spark.sql("alter table fxxcoursedb.fx_course_table set tblproperties('serialization.null.format'='')")

  }

  def readCourseHiveTable(spark: SparkSession): Option[DataFrame] = {
  try{
    logger.warn("readCourseHiveTable method started")
    val courseDF = spark.sql("select * from fxxcoursedb.fx_course_table")
    logger.warn("readCourseHiveTable method ended")
    Some(courseDF)
} catch{
    case e: Exception =>
      logger.error("Error Reading fxxcoursedb.fx_course_table " + e.printStackTrace())
      None
  }
  }

  def writeToHiveTable(spark: SparkSession, df: DataFrame, hiveTable: String): Unit = {
    try {
      logger.warn("writeToHiveTable started")

      val tmpView = hiveTable + "tempView"
      df.createOrReplaceTempView(tmpView)
      //

      val sqlQuery = "create table " + hiveTable + " as select * from " + tmpView

      spark.sql(sqlQuery)
      logger.warn("Finished writing to Hive Table")

    } catch {
      case e: Exception =>
        logger.error("Error writing to Hive Table" + e.printStackTrace())
    }
  }

}
