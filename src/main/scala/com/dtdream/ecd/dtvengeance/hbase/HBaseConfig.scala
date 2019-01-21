package com.dtdream.ecd.dtvengeance.hbase

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf

/**
  * Created by elainetuang on 7/28/16.
  */
object HBaseConfig {
  val config = ConfigFactory.load()

  val hConf = HBaseConfiguration.create()
  hConf.set("hbase.zookeeper.property.clientPort",
    config.getString("hbase.client-port"))

  hConf.set("hbase.zookeeper.quorum",
    config.getString("hbase.quorum"))

  //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
  lazy val conn = ConnectionFactory.createConnection(hConf)

  // JobConf for writing data to HBase
  def getJobConf( tableName: String):JobConf = {
    val jobConf = new JobConf(hConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf
  }
}