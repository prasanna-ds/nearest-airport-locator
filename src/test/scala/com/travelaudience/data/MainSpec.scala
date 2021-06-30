package com.travelaudience.data

import com.travelaudience.data.models.{OptdAirport, UserGeoLocation}
import com.travelaudience.data.utils.SparkUtils.{createSparkSession, getSchema, readCSV}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}

import java.util.{List as JavaList}
import scala.util.Success

trait MainSpec {

  implicit val optdAirportEncoder: Encoder[OptdAirport] = Encoders.product[OptdAirport]
  implicit val userGeoEncoder: Encoder[UserGeoLocation] = Encoders.product[UserGeoLocation]
  implicit val sparkSession: SparkSession               = createSparkSession

  val optdAirportsDf: DataFrame = readCSV[OptdAirport](file = "src/test/resources/inputs/optd-airports.csv")

  def programRunner(usersGeo: JavaList[Row]): DataFrame = {
    val usersGeoDf: DataFrame = sparkSession.createDataFrame(usersGeo, getSchema[UserGeoLocation])

    val geoLocations =
      new NearestGeoLocation(sparkSession, optdAirportsDf, usersGeoDf)

    val nearestAirportDf = geoLocations.findNearestCoordinates() match {
      case Success(nearestAirportDf) =>
        Right(nearestAirportDf).value
    }
    nearestAirportDf
  }

}
