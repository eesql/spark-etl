package com.dtdream.ecd.dtvengeance.tasks

import com.dtdream.ecd.dtvengeance.etl.{ConfigDefinition, EtlDefinition, SparkTask}
import com.dtdream.ecd.dtvengeance.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object WorldCupsTask extends SparkSessionWrapper with SparkTask {

  val spark = getSession()

  def someTransform()(df: DataFrame): DataFrame = {
    val sortedDF = df.sort("Year")

    val transFeatures = Array("GoalsScored", "MatchesPlayed")
    val normDF = MlHelplers.minMaxWrapper(transFeatures, sortedDF)

    normDF.select(
      col("Year") +: (0 until 2).map(i => col("normFeatures")(i).alias(transFeatures(i))): _*
    )
    // Test for bucketing
  }

  override def process(config: ConfigDefinition): Unit = {
    //val writePath = config.targetName
    val spark = getSession()
    val sourceDF = SourceHelplers.getDFfromCSV(spark, config.sourceName)

    val etlDefinition = new EtlDefinition(
      sourceDF = sourceDF,
      transform = someTransform(),
      write = WriteHelplers.write2Jdbc("test_min_max", "jdbc-assets")
    )

    etlDefinition.process()
  }

}


