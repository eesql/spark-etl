package com.dtdream.ecd.dtvengeance.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  def getSession():SparkSession = {
    val spark: SparkSession = {
      SparkSession.builder().master("local").appName("spark session").getOrCreate()
    }
    spark
  }

  def getSessionWithHive(warehouseLocation:String):SparkSession = {
    val spark: SparkSession = {
      SparkSession.builder().master("local").appName("spark session")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport().getOrCreate()
    }
    spark
  }

}
