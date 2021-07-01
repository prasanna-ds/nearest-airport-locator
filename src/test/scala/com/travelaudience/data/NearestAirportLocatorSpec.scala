package com.travelaudience.data

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec

import java.util.{List => JavaList}
import scala.collection.JavaConverters._

class NearestAirportLocatorSpec extends AnyFunSpec with MainSpec {

  describe("a successful run") {

    val nearestAirportDf = codeRunner(Option.empty)

    it("should have the same number of output rows as number of users in input") {
      assert(nearestAirportDf.count() === 20)
    }

    it("should have the expected columns in the output") {
      assert(nearestAirportDf.columns sameElements Array("uuid", "iata_code"))
    }

  }

  describe("finding nearest airport") {

    val usersGeo: JavaList[Row] = Seq(
      Row("31971B3E-2F80-4F8D-86BA-1F2077DF36A2", 35.68500137329102, 139.7514038085938)
    ).asJava

    val nearestAirportDf = codeRunner(Some(usersGeo))

    it("should have the possible nearest airport") {
      assert(nearestAirportDf.collectAsList().get(0).getString(1) == "HND")
    }

    it("should have the distance less than 50 KMS") {

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
      assert(absUserLatitude - absAirportLatitude < 50.0)

    }

  }

  describe("there is no nearest airport because it exceeds maximum distance") {

    val usersGeo: JavaList[Row] = Seq(
      Row("31971B3E-2F80-4F8D-86BA-1F2077DF36A2", 36.2, 139.0)
    ).asJava

    val nearestAirportDf = codeRunner(Some(usersGeo))

    it("should have no iata_code associated to uuid") {
      assert(nearestAirportDf.collectAsList().get(0).getString(1) == null)
    }

  }

}
