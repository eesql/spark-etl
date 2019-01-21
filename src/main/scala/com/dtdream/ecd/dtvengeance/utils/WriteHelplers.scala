package com.dtdream.ecd.dtvengeance.utils

import org.apache.spark.sql.{DataFrame, SaveMode}

object WriteHelplers extends JdbcHelper {

  def write2CSV(writePath:String, partNum:Int = 1)(df:DataFrame):Unit = {
    val path = new java.io.File(writePath).getCanonicalPath
    df.repartition(partNum).write.mode(SaveMode.Overwrite).csv(path)
  }

  def write2Jdbc(tableName:String, dbConfigName:String)(df:DataFrame):Unit = {
    val jdbcProps =  getJdbcProps(dbConfigName)

    df.write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcProps.jdbcUrl, tableName, jdbcProps.connProps)
  }
}
