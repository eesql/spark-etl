package com.dtdream.ecd.dtvengeance.tasks

import com.dtdream.ecd.dtvengeance.etl.{ConfigDefinition, EtlDefinition, SparkTask}
import com.dtdream.ecd.dtvengeance.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object HiveTableSyncTask extends SparkSessionWrapper with SparkTask {

  private val spark = getSessionWithHive("/user/hive/warehouse/legal/")
  private val dbConfigName = "jdbc-legal"


  def someTransform()(df: DataFrame): DataFrame = {
    df
  }


  override def process(config: ConfigDefinition): Unit = {
    val sourceDF = SourceHelplers.getDFfromJdbc(spark, config.sourceName, dbConfigName)

    val etlDefinition = new EtlDefinition(
      sourceDF = sourceDF,
      transform = someTransform(),
      write = WriteHadoopHelplers.write2Hive(config.targetName)
    )

    etlDefinition.process()
  }

}


