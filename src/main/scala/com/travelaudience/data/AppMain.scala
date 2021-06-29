package com.travelaudience.data

import com.travelaudience.data.utils.SparkUtils.{createSparkSession, readCSV, writeCSV}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.annotation.tailrec

object AppMain extends LazyLogging {

  def main(args: Array[String]): Unit = {

    /**
     *

    val usage =
      """
      Usage: search-nearby-airports.jar [--input-file input] [--degrees degrees] [--output-file output]
      """

    val defaultOptions: Map[String, Any] = Map(
      "optd-airports"      -> "",
      "users-geo-location" -> 0,
      "outputFile"         -> ""
    )

    @tailrec
    def parseArgs(list: List[String], options: Map[String, Any]): Map[String, Any] = {
      list match {
        case Nil => options
        case "--input-file" :: value :: tail =>
          parseArgs(tail, options ++ Map("optd-airports" -> value))
        case "--degrees" :: value :: tail =>
          parseArgs(tail, options ++ Map("users-geo-location" -> value.toInt))
        case "--output-file" :: value :: tail =>
          parseArgs(tail, options ++ Map("outputFile" -> value))
        case option :: _ =>
          println("Unknown option " + option)
          println(usage)
          sys.exit(1)
      }
    }

    val options = parseArgs(args.toList, defaultOptions)

    val airportsOPTD = options("optd-airports").asInstanceOf[String]
    val usersGEO     = options("users-geo-location").asInstanceOf[String]
    val outputFile   = options("outputFile").asInstanceOf[String]

    **/

    val sparkSession: SparkSession = createSparkSession("test")

    val usersGeoSchema = StructType(
      List(
        StructField("uuid", StringType, nullable = true),
        StructField("geoip_latitude", DoubleType, nullable = true),
        StructField("geoip_longitude", DoubleType, nullable = true)
      )
    )

    val airportsOPTDSchema = StructType(
      List(
        StructField("iata_code", StringType, nullable = true),
        StructField("latitude", DoubleType, nullable = true),
        StructField("longitude", DoubleType, nullable = true)
      )
    )

    val airportsOPTDDf = readCSV(
      sparkSession,
      "src/main/resources/optd-airports-sample.csv",
      "true",
      ",",
      airportsOPTDSchema
    )

    val usersGeoDf = readCSV(
      sparkSession,
      file = "src/main/resources/user-geo-sample.csv",
      header = "true",
      delimiter = ",",
      schema = usersGeoSchema
    )

    val geoLocations = new NearestGeoLocation(sparkSession, airportsOPTDDf, usersGeoDf.repartition(10))
    val outputDf             = geoLocations.findNearestCoordinates()
    writeCSV(outputDf, "src/main/resources/output/nearby_airports", header = "true", delimiter = ",")

  }

}
