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
      logger.info("Parsing input arguments...")
      list match {
        case Nil => options
        case "--input-files" :: value :: tail =>
          parseArgs(tail, options ++ Map("inputFiles" -> value))
        case "--output-file" :: value :: tail =>
          parseArgs(tail, options ++ Map("outputFile" -> value))
        case option :: _ =>
          logger.error("Unknown option " + option)
          logger.info(usage)
          sys.exit(1)
      }
    }

    val options = parseArgs(args.toList, defaultOptions)

    val inputFiles: Array[String] = options("inputFiles").asInstanceOf[String].split(",")
    val airportsOPTD: String      = inputFiles.head
    val usersGEO: String          = inputFiles(1)
    val outputFile: String        = options("outputFile").asInstanceOf[String]

    logger.info("Reading the coordinates of airports...")
    val optdAirportsDf = readCSV[OptdAirport](file = airportsOPTD)

    logger.info("Reading the user's coordinates...")
    val usersGeoDf =
      readCSV[UserGeoLocation](file = usersGEO)
        .repartition(10) // For production, it has to chosen based on the actual file size and trial and error method

    val nearestAirportLocator = new NearestAirportLocator(optdAirportsDf, usersGeoDf)
    nearestAirportLocator.findNearestAirport() match {
      case Success(nearestAirportsDf) =>
        writeCSV(
          nearestAirportsDf,
          outputFile
        )
      case Failure(exception) =>
        logger.error(s"Nearest airport search failed with exception ${exception.getMessage}")
        sys.exit(1)
    }

  }

}
