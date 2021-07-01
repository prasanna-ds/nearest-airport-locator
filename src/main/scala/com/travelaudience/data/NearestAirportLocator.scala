package com.travelaudience.data

import com.travelaudience.data.models.LocationCoordinate
import com.travelaudience.data.udfs.SparkUdfs.locationMapper
import com.travelaudience.data.utils.SparkUtils.shutdown
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, typedLit}
import org.apache.spark.sql.types.DoubleType

import scala.util.{Failure, Success, Try}

class NearestAirportLocator(masterCoordinatesDf: DataFrame, userCoordinatesDf: DataFrame)(implicit spark: SparkSession)
    extends Serializable
    with LazyLogging {

  private implicit val locationEncoder: Encoder[LocationCoordinate] = Encoders.product[LocationCoordinate]

  def findNearestAirport(): Try[DataFrame] = {

    try {
      val airportCoordinates: Broadcast[List[LocationCoordinate]] = broadcastAirportCoordinates(masterCoordinatesDf)

      logger.info("Starting to find the nearest airport...")
      val nearestAirportsDf = userCoordinatesDf
        .withColumn(
          "nearbyAirport",
          locationMapper(typedLit(airportCoordinates.value), col("uuid"), col("geoip_latitude"), col("geoip_longitude"))
        )
        .select(col("uuid"), col("nearbyAirport.id"))
        .withColumnRenamed("id", "iata_code")

      Success(nearestAirportsDf)
    } catch {
      case exception: Exception =>
        logger.error(s"Exception while finding the nearest airport: $exception")
        logger.warn("Shutting down spark context...")
        shutdown(spark)
        Failure(exception)
    }
  }

  private def broadcastAirportCoordinates(airportCoordinatesDf: DataFrame): Broadcast[List[LocationCoordinate]] = {

    logger.info("Broadcasting the Coordinates of Airports...")
    val broadcastVariable = airportCoordinatesDf
      .withColumn("lat", col("latitude").cast(DoubleType))
      .withColumn("long", col("longitude").cast(DoubleType))
      .drop("latitude,longitude")
      .filter(col("lat")  > -90.0 && col("lat") < 90.0) // filtering invalid latitudes
      .filter(col("long")  > -180.0 && col("long") < 180.0) // filtering invalid longitudes
      .coalesce(numPartitions = 1)
      .sort(col("lat").asc, col("long").asc)
      .map { row: Row =>
        LocationCoordinate(row.getString(0), row.getDouble(1), row.getDouble(2))
      }
      .collect()
      .toList

    spark.sparkContext.broadcast(broadcastVariable)

  }

}
