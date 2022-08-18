import common.{FXJsonParser, InputConfig, PostgresCommon, SparkCommon, SparkDataTransformer}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.Properties

object SparkTransformer {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
   try{
     val arg_length = args.length
     if (arg_length == 0) {
       System.out.println("No Argument passed..exiting")
       System.exit(1)
     }

     val inputConfig : InputConfig = InputConfig(env = args(0),targetDB = args(1))
     /*val env_name : String = args(0)
     System.out.println("Environment is  " +env_name)
     logger.info("Main method started")
     logger.warn("This is a dummy warning")*/
     val spark: SparkSession = SparkCommon.createSparkSession(inputConfig).get
      //Create Hive table
     //SparkCommon.createCourseHiveTable(spark)

     val CourseDF = SparkCommon.readCourseHiveTable(spark).get
     CourseDF.show()

     // Replace Null Value

     val transformedDF1 = SparkDataTransformer.replaceNullValues(CourseDF)
     transformedDF1.show()

     if(inputConfig.targetDB == "pg"){
       logger.info("Writing to pg table ")
       val pgCourseTable = FXJsonParser.fetchPGTargetTable()
       logger.warn("******** pgCourseTable **** is " + pgCourseTable)
       PostgresCommon.writeDFToPostgresTable(transformedDF1,pgCourseTable)
     }

     if (inputConfig.targetDB == "hive") {
       logger.info("Writing to CSV File ")

       transformedDF1.write.format("csv").save("transformed-df1")

       logger.info("Writing to Hive Table")
       // Write to a Hive Table
       SparkCommon.writeToHiveTable(spark, transformedDF1, "customer_transformed1")
       logger.info("Finished writing to Hive Table..in main method")

     }
     //val pgCourseTable = "futurexschema.futurex_course"
     //val pgCourseTable = FXJsonParser.fetchPGTargetTable()
     //Code for writing data to Postgres table
     //logger.warn("******** pgCourseTable **** is " + pgCourseTable)
     //PostgresCommon.writeDFToPostgresTable(transformedDF1,pgCourseTable)



     //logger.info("Fetched")
     //pgCourseDataframe.show()
     //logger.info("Shown")
   }catch {
     case e:Exception =>
       logger.error("An error has occured in main method "+e.printStackTrace())
   }
  }

}
