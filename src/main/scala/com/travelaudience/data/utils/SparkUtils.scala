package com.travelaudience.data.utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoder, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag


object SparkUtils {

  /**
   * Create Spark Session
   * @return Spark Session
   */
  def createSparkSession(appName: String): SparkSession = {
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
   * @param header Has header. "true" or "false"
   * @param delimiter Type of delimiter. "," or "\t"
   * @return Dataframe.
   */
  def readCSV[T: Encoder: TypeTag](
      sparkSession: SparkSession,
      file: String,
      header: String,
      delimiter: String,
  ): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("delimiter", delimiter)
      .option("inferSchema", "false")
      .option("header", header)
      .schema(getSchema[T])
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
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", header)
      .option("delimiter", delimiter)
      .option("quote", "\u0000")
      .save(outputLocation)
  }


  def getSchema[T: TypeTag]: StructType = {
    ScalaReflection
      .schemaFor[T]
      .dataType
      .asInstanceOf[StructType]
  }
}
