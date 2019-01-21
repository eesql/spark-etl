package com.dtdream.ecd.dtvengeance.tasks

import com.dtdream.ecd.dtvengeance.etl.{ConfigDefinition, SparkTask}
import com.dtdream.ecd.dtvengeance.hbase.{HBaseConfig, HBaseHelper}

object HBaseTestTask extends SparkTask with HBaseHelper {

  override def process(config: ConfigDefinition): Unit = {
    //dropHTable(config.sourceName)

    try {
      createTestTable
    } catch {
      case e: Exception => e.printStackTrace
    }
    HBaseConfig.conn.close()
  }
}
