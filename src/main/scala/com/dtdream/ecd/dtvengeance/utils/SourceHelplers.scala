package com.dtdream.ecd.dtvengeance.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceHelplers extends LazyLogging with JdbcHelper {

  def getDFfromCSV(spark:SparkSession, path:String, header:String="true"):DataFrame = {
    val dataPath: String = {
      new java.io.File(path).getCanonicalPath
    }

    val resultDF = spark.read.format("csv").option("header", header).option("inferSchema","true").load(dataPath)
    //resultDF.printSchema()
    resultDF
  }

  def getDFfromJdbc(spark:SparkSession, table:String, dbConfigName:String):DataFrame = {
    val jdbcProps =  getJdbcProps(dbConfigName)
    getDfFromTable(spark, table, jdbcProps.jdbcUrl, jdbcProps.connProps )
  }
}
