package com.travelaudience.data

import com.travelaudience.data.models.{OptdAirport, UserGeoLocation}
import com.travelaudience.data.utils.SparkUtils.{createSparkSession, getSchema, readCSV}
import org.apache.spark.sql._

import java.util.{List => JavaList}
import scala.util.Success

trait MainSpec {

  implicit val optdAirportEncoder: Encoder[OptdAirport] = Encoders.product[OptdAirport]
  implicit val userGeoEncoder: Encoder[UserGeoLocation] = Encoders.product[UserGeoLocation]
  implicit val sparkSession: SparkSession               = createSparkSession

  val optdAirportsDf: DataFrame = readCSV[OptdAirport](file = "src/test/resources/inputs/optd-airports.csv")
  var usersGeoDf: DataFrame     = readCSV[UserGeoLocation](file = "src/test/resources/inputs/user-geo.csv")

  def codeRunner(usersGeo: Option[JavaList[Row]]): DataFrame = {

    if(usersGeo.nonEmpty) {
      usersGeoDf = sparkSession.createDataFrame(usersGeo.get, getSchema[UserGeoLocation])
    }

    val geoLocations =
      new NearestAirportLocator(sparkSession, optdAirportsDf, usersGeoDf)

    val nearestAirportDf = geoLocations.findNearestAirport() match {
      case Success(nearestAirportDf) =>
        Right(nearestAirportDf).value
    }
    nearestAirportDf
  }

}
