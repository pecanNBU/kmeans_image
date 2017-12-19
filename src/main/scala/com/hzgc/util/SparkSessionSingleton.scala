package com.hzgc.util

import java.io.File

import org.apache.spark.sql.SparkSession

/**
  * SparkSession single instance model
  */
object SparkSessionSingleton {
  private var instance: SparkSession = _
  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  def getInstance(): SparkSession = {
    if (instance == null) {
      instance = SparkSession.builder()
        .appName("kmeans-clustering")
        .master("local[*]")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .getOrCreate()
    }
    instance
  }
}
