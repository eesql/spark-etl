package com.dtdream.ecd.dtvengeance.utils

import com.typesafe.config.ConfigFactory
import java.sql.DriverManager
import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

trait JdbcHelper {

  case class JdbcProps(
                        jdbcHostname:String,
                        jdbcPort:String,
                        jdbcDatabase:String,
                        jdbcUrl:String,
                        connProps:Properties
                      )

  def getJdbcProps(configName: String):JdbcProps = {
    val config = ConfigFactory.load()
    val jdbcDbType = config.getString(configName + ".db_type")
    val jdbcHostname = config.getString(configName + ".hostname")
    val jdbcPort = config.getString(configName + ".port")
    val jdbcDatabase = config.getString(configName + ".db")
    val jdbcUsername = config.getString(configName + ".username")
    val jdbcPassword = config.getString(configName + ".password")

    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    val jdbcUrl = s"jdbc:${jdbcDbType}://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    JdbcProps(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUrl, connectionProperties)
  }

  def getJdbcConn(props:JdbcProps):Connection = {
    // Create the JDBC URL without passing in the user and password parameters

    val connection = DriverManager.getConnection(props.jdbcUrl,
      props.connProps.getProperty("user"), props.connProps.getProperty("password"))
    connection
  }

  def getDfFromTable(spark:SparkSession, tableName:String, jdbcUrl:String, connProps:Properties):DataFrame = {
    val df = spark.read.jdbc(jdbcUrl, tableName, connProps)
    df
  }


}
