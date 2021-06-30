package com.travelaudience.data.udfs

import com.travelaudience.data.models.LocationCoordinate
import com.travelaudience.data.utils.AppUtils.calculateDistanceInKilometer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.annotation.tailrec

object SparkUdfs extends Serializable {

  def locationMapper: UserDefinedFunction =
    udf{(broadcastCoordinates: List[LocationCoordinate], searchId: String, searchLat: Double, searchLon: Double) => {

      val latLimit = 0.5
      val maxDist  = 50.0

      @tailrec
      def binarySearchCoordinates(min: Int, max: Int): (Int, Int) = {
        if(min >= max) return (min, max)
        val middle = (min + max) / 2
        broadcastCoordinates match {
          case coordinate: List[LocationCoordinate] if searchLat - latLimit > coordinate(middle).latitude =>
            binarySearchCoordinates(middle + 1, max)
          case coordinate: List[LocationCoordinate] if searchLat + latLimit < coordinate(middle).latitude =>
            binarySearchCoordinates(min, middle - 1)
          case _ => (min, max)
        }
      }
      val (min, max) = binarySearchCoordinates(0, broadcastCoordinates.size - 1)
      val nearByCoordinates  = broadcastCoordinates.slice(min, max + 1)

      nearByCoordinates match {
        case matchedCoordinate if nearByCoordinates.nonEmpty =>
          val userCoordinate = LocationCoordinate(searchId, searchLat, searchLon)
          val nearestCoordinate = nearByCoordinates
            .map { masterCoordinate =>
              (masterCoordinate, calculateDistanceInKilometer(userCoordinate, masterCoordinate))
            }
            .reduce((a, b) => if (a._2 < b._2) a else b)
          if (nearestCoordinate._2 < maxDist)
            Some(nearestCoordinate._1)
          else
            None
        case _ =>
          None
      }
    }}

}
