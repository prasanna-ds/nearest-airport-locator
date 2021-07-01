package com.travelaudience.data.udfs

import com.travelaudience.data.models.LocationCoordinate
import com.travelaudience.data.utils.NearestAirportLocatorUtils.calculateDistanceInKilometer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.annotation.tailrec

object SparkUdfs extends Serializable {

  def locationMapper: UserDefinedFunction =
    udf { (masterCoordinates: List[LocationCoordinate], userUUID: String, userLat: Double, userLon: Double) =>
      {

        val latLimit = 0.5
        val lonLimit = 4.0
        val maxDistanceLimitInKms  = 50.0 // max difference in distance to assume a nearest airport.

        @tailrec
        def binarySearchCoordinates(min: Int, max: Int): (Int, Int) = {
          if(min >= max) return (min, max)
          val middle = (min + max) / 2
          masterCoordinates match {
            case coordinate: List[LocationCoordinate] if userLat - latLimit > coordinate(middle).latitude =>
              binarySearchCoordinates(middle + 1, max)
            case coordinate: List[LocationCoordinate] if userLat + latLimit < coordinate(middle).latitude =>
              binarySearchCoordinates(min, middle - 1)
            case _ => (min, max)
          }
        }

        // Start with Latitudes by filtering the nearby airport coordinates which are beyond +-latLimit
        // Also possible doing with Longitudes first...
        val (min, max)        = binarySearchCoordinates(0, masterCoordinates.size - 1)
        val nearByCoordinates = masterCoordinates.slice(min, max + 1)

        // Additionally filter on longitude
        val filteredNearbyCoordinates = nearByCoordinates.filter { coordinate =>
          (userLon + lonLimit > coordinate.longitude
          && userLon - lonLimit < coordinate.longitude)
        }

        // Now that we have the coordinates which are nearer to the user's coordinate, we can use haversine formula to
        // calculate the distance between two points.
        if(filteredNearbyCoordinates.nonEmpty) {
          val userCoordinate = LocationCoordinate(userUUID, userLat, userLon)
          val nearestCoordinate = nearByCoordinates
            .map { masterCoordinate =>
              (masterCoordinate, calculateDistanceInKilometer(userCoordinate, masterCoordinate))
            }
            .reduce((a, b) => if(a._2 < b._2) a else b)
          if(nearestCoordinate._2 < maxDistanceLimitInKms)
            Some(nearestCoordinate._1)
          else
            None
        } else {
          None
        }

      }
    }

}
