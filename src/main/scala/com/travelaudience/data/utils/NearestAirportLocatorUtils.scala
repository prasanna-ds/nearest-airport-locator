package com.travelaudience.data.utils

import com.travelaudience.data.models.LocationCoordinate

object NearestAirportLocatorUtils {
  val RADIUS_OF_EARTH_IN_KM = 6371

  /**
   * Haversine formula to calculate the distance between two geographical points.
   * @param userCoordinate Coordinates of a User
   * @param masterCoordinate Coordinates of a Airport
   * @return
   */
  def calculateDistanceInKilometer(
      userCoordinate: LocationCoordinate,
      masterCoordinate: LocationCoordinate
  ): Int = {
    val latDistance = Math.toRadians(userCoordinate.latitude - masterCoordinate.latitude)
    val lngDistance = Math.toRadians(userCoordinate.longitude - masterCoordinate.longitude)
    val sinLat      = Math.sin(latDistance / 2)
    val sinLng      = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(userCoordinate.latitude)) *
        Math.cos(Math.toRadians(masterCoordinate.latitude)) *
        sinLng * sinLng)
    val b = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (RADIUS_OF_EARTH_IN_KM * b).toInt
  }

}
