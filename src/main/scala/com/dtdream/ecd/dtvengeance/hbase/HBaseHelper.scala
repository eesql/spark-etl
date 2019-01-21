package com.dtdream.ecd.dtvengeance.hbase

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.regionserver.{BloomType, ConstantSizeRegionSplitPolicy}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

trait HBaseHelper extends LazyLogging {
  def createHTable: Unit = {

  }

  def dropHTable(name: String): Unit = {
    val tableName = TableName.valueOf(name)
    val admin = HBaseConfig.conn.getAdmin

    if (!admin.tableExists(tableName)) {
      logger.warn(name + " table does not exists, skip dropping")
    } else {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      logger.info(name + " table deleted.+++++++++++++++++++++++")
    }
  }

  def createTestTable: Unit = {
    val regionCount = 3
    val numOfSalts = 3

    val admin = HBaseConfig.conn.getAdmin
    logger.info("get admin. +++++++++++++++++++++++++++++++++")

    val tableDescriptor = new HTableDescriptor(TableName.valueOf("scala_test"))
    val columnDescriptor = new HColumnDescriptor("cf".getBytes())

    logger.info("created descriptor. +++++++++++++++++++++++++++++++++")

    //columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY)
    columnDescriptor.setBlocksize(64 * 1024)
    columnDescriptor.setBloomFilterType(BloomType.ROW)

    tableDescriptor.addFamily(columnDescriptor)

    tableDescriptor.setMaxFileSize(Long.MaxValue)
    tableDescriptor.setRegionSplitPolicyClassName(classOf[ConstantSizeRegionSplitPolicy].getName)

    logger.info("descriptor attributes config. +++++++++++++++++++++++++++++++++")

    /**
    val splitKeys = new mutable.MutableList[Array[Byte]]
    for (i <- 0 to regionCount) {
      val regionSplitStr = StringUtils.leftPad((i*(numOfSalts/regionCount)).toString, 4, "0")
      splitKeys += Bytes.toBytes(regionSplitStr)
    }**/

    admin.createTable(tableDescriptor)
    logger.info("table created successfully. ++++++++++++++++++++++++++++")
  }
}
