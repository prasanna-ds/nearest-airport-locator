package com.travelaudience.data

import com.travelaudience.data.models.LocationCoordinate
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql._

import scala.annotation.tailrec


class NearestGeoLocation(spark: SparkSession, val masterCoordinatesDf: DataFrame, val userCoordinatesDf: DataFrame)
    extends Serializable
    with LazyLogging {

  implicit val movieEncoder: Encoder[LocationCoordinate] = Encoders.product[LocationCoordinate]

  val latLimit = 0.5
  val lonLimit = 4.0
  val maxDist  = 50.0

  val AVERAGE_RADIUS_OF_EARTH_KM = 6371

  def findNearestCoordinates(): DataFrame = {

    val masterCoordinates: Broadcast[List[LocationCoordinate]] = broadcastMasterCoordinates(masterCoordinatesDf)
    val index: List[LocationCoordinate]                        = masterCoordinates.value

    val locationMapper = (searchId: String, searchLat: Double, searchLon: Double) => {


      val filteredCoordinates = index.filter{ coordinate =>
        val masterLat       = coordinate.latitude
        val masterLongitude = coordinate.longitude
        (searchLat - latLimit < masterLat || searchLat + latLimit > masterLat) &&
        (searchLon - lonLimit < masterLongitude || searchLon + lonLimit < masterLongitude)
      }

    /*
      @tailrec
      def binarySearchCoordinates(min: Int, max: Int): (Int, Int) = {
        if (min >= max) return (min, max)
        val middle = (min + max) / 2
        index match {
          case coordinate: List[LocationCoordinate] if searchLat - latLimit > coordinate(middle).latitude => binarySearchCoordinates(middle + 1, max)
          case coordinate: List[LocationCoordinate] if searchLat + latLimit < coordinate(middle).latitude => binarySearchCoordinates(min, middle - 1)
          case _ => (min, max)
        }
      }
      val (min, max) = binarySearchCoordinates(0, index.size - 1)
      val slicedIdx = index.slice(min, max + 1)

     */

      filteredCoordinates match {
        case matchedCoordinate =>
          val userCoordinate = LocationCoordinate(searchId, searchLat, searchLon)
          val nearestCoordinate = filteredCoordinates
            .map { masterCoordinate =>
              (masterCoordinate, calculateDistanceInKilometer(userCoordinate, masterCoordinate))
            }
            .reduce((a, b) => if(a._2 < b._2) a else b)
          if(nearestCoordinate._2 < maxDist)
            Some(nearestCoordinate._1)
          else
            None
        case _ =>
          None
      }
    }

    val locationMapperUdf = udf(locationMapper)
    val outputdf = userCoordinatesDf
      .withColumn("nearbyAirport", locationMapperUdf(col("uuid"), col("geoip_latitude"), col("geoip_longitude")))

    outputdf.select(col("uuid"), col("nearbyAirport.id"))

  }

  private def broadcastMasterCoordinates(masterCoordinatesDf: DataFrame): Broadcast[List[LocationCoordinate]] = {

    val broadcastVariable = masterCoordinatesDf
      .withColumn("lat", col("latitude").cast(DoubleType))
      .withColumn("long", col("longitude").cast(DoubleType))
      .drop("latitude,longitude")
      .coalesce(numPartitions = 1)
      .sort(col("lat").asc, col("long").asc)
      .map { row: Row =>
        LocationCoordinate(row.getString(0), row.getDouble(1), row.getDouble(2))
      }
      .collect()
      .toList

    spark.sparkContext.broadcast(broadcastVariable)

  }

  private def calculateDistanceInKilometer(userCoordinate: LocationCoordinate, masterCoordinate: LocationCoordinate): Int = {
    val latDistance = Math.toRadians(userCoordinate.latitude - masterCoordinate.latitude)
    val lngDistance = Math.toRadians(userCoordinate.longitude - masterCoordinate.longitude)
    val sinLat      = Math.sin(latDistance / 2)
    val sinLng      = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(userCoordinate.latitude)) *
        Math.cos(Math.toRadians(masterCoordinate.latitude)) *
        sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
  }

}
