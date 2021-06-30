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

class NearestGeoLocation(spark: SparkSession, masterCoordinatesDf: DataFrame, userCoordinatesDf: DataFrame)
    extends Serializable
    with LazyLogging {

  private implicit val locationEncoder: Encoder[LocationCoordinate] = Encoders.product[LocationCoordinate]

  def findNearestCoordinates(): Try[DataFrame] = {

    try {
      val masterCoordinates: Broadcast[List[LocationCoordinate]] = broadcastMasterCoordinates(masterCoordinatesDf)

      val nearByAirportDf = userCoordinatesDf
        .withColumn(
          "nearbyAirport",
          locationMapper(typedLit(masterCoordinates.value), col("uuid"), col("geoip_latitude"), col("geoip_longitude"))
        )
        .select(col("uuid"), col("nearbyAirport.id"))
        .withColumnRenamed("id", "iata_code")

      Success(nearByAirportDf)
    } catch {
      case exception: Exception =>
        logger.error(s"exception: $exception")
        shutdown(spark)
        Failure(exception)
    }
  }

  private def broadcastMasterCoordinates(masterCoordinatesDf: DataFrame): Broadcast[List[LocationCoordinate]] = {

    val broadcastVariable = masterCoordinatesDf
      .withColumn("lat", col("latitude").cast(DoubleType))
      .withColumn("long", col("longitude").cast(DoubleType))
      .drop("latitude,longitude")
      .filter(col("lat")  > -90.0 && col("lat") < 90.0)
      .filter(col("long")  > -180.0 && col("long") < 180.0)
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
