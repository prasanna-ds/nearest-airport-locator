package com.travelaudience.data

import com.travelaudience.data.models.{OptdAirport, UserGeoLocation}
import com.travelaudience.data.utils.SparkUtils.{createSparkSession, readCSV, writeCSV}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Success}

object AppMain extends LazyLogging {

  implicit val optdAirportEncoder: Encoder[OptdAirport] = Encoders.product[OptdAirport]
  implicit val userGeoEncoder: Encoder[UserGeoLocation] = Encoders.product[UserGeoLocation]
  implicit val sparkSession: SparkSession               = createSparkSession

  def main(args: Array[String]): Unit = {

    val usage =
      """
      Usage: find-nearby-airport.jar [--input-files inputFiles] [--output-file outputFile]
      """

    val defaultOptions: Map[String, Any] = Map(
      "inputFiles" -> "",
      "outputFile" -> ""
    )

    @tailrec
    def parseArgs(list: List[String], options: Map[String, Any]): Map[String, Any] = {
      list match {
        case Nil => options
        case "--input-files" :: value :: tail =>
          parseArgs(tail, options ++ Map("inputFiles" -> value))
        case "--output-file" :: value :: tail =>
          parseArgs(tail, options ++ Map("outputFile" -> value))
        case option :: _ =>
          println("Unknown option " + option)
          println(usage)
          sys.exit(1)
      }
    }

    val options = parseArgs(args.toList, defaultOptions)

    val inputFiles: Array[String] = options("inputFiles").asInstanceOf[String].split(",")
    val airportsOPTD: String      = inputFiles.head
    val usersGEO: String          = inputFiles(1)
    val outputFile: String        = options("outputFile").asInstanceOf[String]

    val optdAirportsDf = readCSV[OptdAirport](file = airportsOPTD)
    val usersGeoDf     = readCSV[UserGeoLocation](file = usersGEO)

    val geoLocations = new NearestGeoLocation(sparkSession, optdAirportsDf, usersGeoDf.repartition(10))
    geoLocations.findNearestCoordinates() match {
      case Success(nearByAirportDf) =>
        writeCSV(
          nearByAirportDf,
          outputFile
        )
      case Failure(exception) =>
        logger.error(s"Nth Degree calculation failed with exception ${exception.getMessage}")
        sys.exit(1)
    }

  }

}
