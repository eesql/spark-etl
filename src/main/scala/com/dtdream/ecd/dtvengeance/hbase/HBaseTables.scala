package com.dtdream.ecd.dtvengeance.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, RowFilter, SubstringComparator}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

/**
  * Created by elainetuang on 7/28/16.
  */
object HBaseTables {
  val testTable = TableName.valueOf("t_realtime_kpi_bus_channel")
  val visitRealTable = TableName.valueOf("behavior_visit_realtime")
  val locOrderTable = TableName.valueOf("order_realtime")
  val stationOrderTable = TableName.valueOf("t_realtime_kpi_bus_station")

  def convertTestOrder(triple: (String, Int)) = {
    val p = new Put(Bytes.toBytes(triple._1))
    p.addColumn(Bytes.toBytes("p"),Bytes.toBytes("order"),Bytes.toBytes(triple._2.toString))
    (new ImmutableBytesWritable, p)
  }

  /**
    * 获取整合实时kpi数据,包括订单指标和当日累积pv uv指标
    */
  def getDailyKPI(today:String):List[List[String]] = {
    val orderKPI = getHDailyOrders(today)
    val pvKPI = getDailyPV(today.replace("-",""))

    List(orderKPI(0) , pvKPI(0))
  }


  def getHDailyOrders(today:String):List[List[String]] = {

    //setup filter
    val rowFilter = new RowFilter(CompareOp.EQUAL,
      new SubstringComparator(today+" "))

    val results = getRealtimeOrders(rowFilter)

    if (results(0)(1) != "NaN") {
      List(List(today, results.map(x => x(1).toInt).sum[Int].toString))
    } else { results }
  }


  /** 查询分钟级别渠道订单指标 **/
  def getHMinOrders(today:String):List[List[String]] = {

    //setup filter
    val rowFilter = new RowFilter(CompareOp.EQUAL,
      new SubstringComparator(today+" "))

    getRealtimeOrders(rowFilter)
  }


  /** 根据不同filter查询test_realtime订单数据 */
  def getRealtimeOrders( filter:RowFilter):List[List[String]] = {

    val table = HBaseConfig.conn.getTable(testTable)

    //scan data
    val scan = new Scan()
    scan.addColumn("p".getBytes, "order".getBytes)

    scan.setFilter(filter)

    val scanner = table.getScanner(scan)

    try {

      val results = scanner.iterator().asScala.map( r =>
        List(Bytes.toString(r.getRow()),
          Bytes.toString( r.getValue("p".getBytes, "order".getBytes)
          ))
      )

      //未找到对应ROW KEY时返回空数据标识
      if (results.isEmpty) { List(List("NaN", "NaN")) } else { results.toList }
    }
    finally {
      scanner.close()
    }

  }


  /** 获取当日累积pv app打开指标  **/
  def getDailyPV(today:String):List[List[String]] = {
    val table = HBaseConfig.conn.getTable(visitRealTable)

    //setup filter
    val rowFilter = new RowFilter(CompareOp.EQUAL,
      new BinaryComparator(Bytes.toBytes(today)))

    val scan = new Scan()
    scan.addColumn("index".getBytes, "pv".getBytes)
    scan.addColumn("index".getBytes, "app".getBytes)

    scan.setFilter(rowFilter)

    val scanner = table.getScanner(scan)

    try {
      val results = scanner.iterator().asScala.map( r =>
        List(Bytes.toString(r.getRow()),
          Bytes.toString(r.getValue("index".getBytes, "pv".getBytes)),
          Bytes.toString(r.getValue("index".getBytes, "app".getBytes))
        )
      )
      //未找到对应ROW KEY时返回空数据标识
      if (results.isEmpty) { List(List("NaN", "NaN", "NaN")) } else { results.toList }
    }
    finally {
      scanner.close()
    }
  }

  /** 获取实时pv指标  **/
  def getRealtimePV(today:String):List[List[String]] = {
    val table = HBaseConfig.conn.getTable(visitRealTable)

    //setup filter
    val rowFilter = new RowFilter(CompareOp.EQUAL,
      new SubstringComparator("PERIOD_"+today.replace("-","")))

    val scan = new Scan()
    scan.addColumn("index".getBytes, "pv".getBytes)

    scan.setFilter(rowFilter)

    val scanner = table.getScanner(scan)

    try {
      val results = scanner.iterator().asScala.map( r =>
        List(Bytes.toString(r.getRow()),
          Bytes.toString(r.getValue("index".getBytes, "pv".getBytes))
        )
      )
      //未找到对应ROW KEY时返回空数据标识
      if (results.isEmpty) { List(List("NaN", "NaN", "NaN")) } else { results.toList }
    }
    finally {
      scanner.close()
    }
  }


  /** 获取客运站级别订单数据指标 **/
  def getStationOrders(today:String):List[List[String]] = {
    val table = HBaseConfig.conn.getTable(stationOrderTable)

    //setup filter
    val rowFilter = new RowFilter(CompareOp.EQUAL,
      new SubstringComparator(today+"d"))

    val scan = new Scan()
    scan.addColumn("p".getBytes, "tct".getBytes)
    scan.addColumn("p".getBytes, "mon".getBytes)

    scan.setFilter(rowFilter)

    val scanner = table.getScanner(scan)

    try {
      val results = scanner.iterator().asScala.map( r =>
        List(Bytes.toString(r.getRow()).split('.')(0),
          Bytes.toString(r.getValue("p".getBytes, "tct".getBytes)),
          Bytes.toString(r.getValue("p".getBytes, "mon".getBytes))
        )
      )
      //未找到对应ROW KEY时返回空数据标识
      results.toList
    } finally {
      scanner.close()
    }


  }


}

