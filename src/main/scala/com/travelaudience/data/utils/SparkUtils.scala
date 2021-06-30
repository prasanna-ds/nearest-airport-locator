package com.travelaudience.data.utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoder, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

object SparkUtils {

  val APP_NAME = "nearest-airport-finder"

  /**
   * Create Spark Session
   * @return Spark Session
   */
  val createSparkSession: SparkSession = {
   val sparkSession=  SparkSession
      .builder()
      .appName(APP_NAME)
      .master("local[*]")
      .getOrCreate()

    val hadoopConfig = sparkSession.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    sparkSession
  }

  /**
   * Read File and create dataframe
   * @param file Input File with location.
   * @param header Has header. "true" or "false"
   * @param delimiter Type of delimiter. "," or "\t"
   * @return Dataframe.
   */
  def readCSV[T: Encoder: TypeTag](file: String, header: String = "true", delimiter: String = ",")(implicit
      spark: SparkSession
  ): DataFrame = {
    spark.read
      .format("csv")
      .option("delimiter", delimiter)
      .option("inferSchema", "false")
      .option("header", header)
      .schema(getSchema[T])
      .load(file)
  }

  def getSchema[T: TypeTag]: StructType = {
    ScalaReflection
      .schemaFor[T]
      .dataType
      .asInstanceOf[StructType]
  }

  /**
   * Write Dataframe as CSV file.
   * @param dataFrame Dataframe to write to a file.
   * @param outputLocation Output Location.
   * @param header Header to be written or not. "true" or "false"
   * @param delimiter Type of delimiter. "," or "\t"
   */
  def writeCSV(dataFrame: DataFrame, outputLocation: String, header: String = "true", delimiter: String = ","): Unit = {
    dataFrame.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", header)
      .option("delimiter", delimiter)
      .option("quote", "\u0000")
      .save(outputLocation)
  }

  def shutdown(sparkSession: SparkSession): Unit = {
    sparkSession.stop()
  }

}
