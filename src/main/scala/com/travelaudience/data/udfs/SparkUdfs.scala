package com.travelaudience.data.udfs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array_except, udf}

object SparkUdfs {
  def appendToSeq: UserDefinedFunction = udf((x: Seq[String], y: String) => (x ++ Seq(y)).distinct)

  def mergeSeq: UserDefinedFunction =
    udf((x: Seq[String], y: Seq[String]) => {
      if(y != null) (x ++ y).distinct else x.distinct
    })

  def flattenSeq: UserDefinedFunction = udf((x: Seq[Seq[String]]) => x.flatten.distinct.sorted)
}
