package com.travelaudience.data

import com.travelaudience.data.models.{OptdAirport, UserGeoLocation}
import com.travelaudience.data.utils.SparkUtils.{createSparkSession, getSchema, readCSV}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec

import java.util
import scala.collection.JavaConverters._
import scala.util.Success

class MainSpec extends AnyFunSpec with BeforeAndAfter {

  implicit val optdAirportEncoder: Encoder[OptdAirport] = Encoders.product[OptdAirport]
  implicit val userGeoEncoder: Encoder[UserGeoLocation] = Encoders.product[UserGeoLocation]
  implicit val sparkSession: SparkSession               = createSparkSession

  val optdAirportsDf: DataFrame = readCSV[OptdAirport](file = "src/test/resources/inputs/optd-airports.csv")

  describe("a successful run") {

    val usersGeo: util.List[Row] = Seq(
      Row("DDEFEBEA-98ED-49EB-A4E7-9D7BFDB7AA0B", -37.83330154418945, 145.0500030517578),
      Row("DAEF2221-14BE-467B-894A-F101CDCC38E4", 52.51670074462891, 4.666699886322021),
      Row("31971B3E-2F80-4F8D-86BA-1F2077DF36A2", 35.68500137329102, 139.7514038085938)
    ).asJava

    val nearestAirportDf = programRunner(usersGeo)

    it("should have the same number of output rows as number of users in input") {
      assert(nearestAirportDf.count() === 3)
    }

    it("should have the expected columns in the output") {
      assert(nearestAirportDf.columns sameElements Array("uuid", "iata_code"))
    }

  }

  describe("finding nearest airport") {

    val usersGeo: util.List[Row] = Seq(
      Row("31971B3E-2F80-4F8D-86BA-1F2077DF36A2", 35.68500137329102, 139.7514038085938)
    ).asJava

    val nearestAirportDf = programRunner(usersGeo)

    it("should have the possible nearest airport") {
      assert(nearestAirportDf.collectAsList().get(0).getString(1) == "HND")
    }

    it("should have the correct possible nearest airport") {
      val usersGeoDf = sparkSession.createDataFrame(usersGeo, getSchema[UserGeoLocation])

      val absUserLatitude = math.abs(
        usersGeoDf
          .filter(col("uuid") === nearestAirportDf.collectAsList().get(0).getString(0))
          .collectAsList()
          .get(0)
          .getDouble(1)
      )

      val absAirportLatitude = math.abs(
        optdAirportsDf
          .filter(col("iata_code") === "HND")
          .collectAsList()
          .get(0)
          .getDouble(1)
      )
      assert(
        absUserLatitude - absAirportLatitude < 50.0
      )

    }

  }

  def programRunner(usersGeo: util.List[Row]): DataFrame = {
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
