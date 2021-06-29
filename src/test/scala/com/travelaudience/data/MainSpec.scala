package com.travelaudience.data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat_ws, size}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funspec.AnyFunSpec

import scala.util.Success

class MainSpec extends AnyFunSpec {

  var sparkSession: SparkSession = _
  var inputDf: DataFrame         = _

  sparkSession = SparkSession
    .builder()
    .appName("near_by_airports_test")
    .config("spark.testing.memory", "2147480000")
    .master("local[*]")
    .getOrCreate()

  val rootLogger: Logger = Logger.getLogger(getClass)
  rootLogger.setLevel(Level.ERROR)
}
