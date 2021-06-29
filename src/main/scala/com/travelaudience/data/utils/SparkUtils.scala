package com.travelaudience.data.utils

import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  /**
   * Create Spark Session
   * @return Spark Session
   */
  lazy val sparkSession: String => SparkSession = (appName: String) => {
      SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

  }

  /**
   * Read File and create dataframe
   * @param sparkSession Spark Session.
   * @param file Input File with location.
   * @param header Header to be written or not. "true" or "false"
   * @param delimiter Type of delimiter. "," or "\t"
   * @param schema Schema of the file.
   * @return Dataframe.
   */
  def readCSV(
      sparkSession: SparkSession = sparkSession,
      file: String,
      header: String,
      delimiter: String,
      schema: StructType
  ): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("delimiter", delimiter)
      .option("header", header)
      .schema(schema)
      .load(file)
  }

  /**
   * Write Dataframe as CSV file.
   * @param dataFrame Dataframe to write to a file.
   * @param outputLocation Output Location.
   * @param header Header to be written or not. "true" or "false"
   * @param delimiter Type of delimiter. "," or "\t"
   */
  def writeCSV(dataFrame: DataFrame, outputLocation: String, header: String, delimiter: String): Unit = {
    dataFrame
    // setting repartition as 1 as this is an exercise, but it will be an expensive operation if there is a lot of data.
    // If we do not change the number of partitions, there will be multiple files, but data in each file will be sorted
      //.coalesce(1)
      .write
      .format("csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .option("quote", "\u0000")
      .save(outputLocation)
  }

}
