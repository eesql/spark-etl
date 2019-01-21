package com.dtdream.ecd.dtvengeance.tasks

import com.dtdream.ecd.dtvengeance.etl.{ConfigDefinition, EtlDefinition, SparkTask}
import com.dtdream.ecd.dtvengeance.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object AssetModelTask extends SparkSessionWrapper with SparkTask {

  private val spark = getSession()
  private val dbConfigName = "jdbc-assets"

  private val CONFIG_TABLE = "t_asset_eval_global_config"
  private val CONFIG_COL = "config_metric_value"


  def someTransform()(df: DataFrame): DataFrame = {

    // define which columns needed to be normalized
    val transFeatures = Array("cost_index", "stability_index", "timeliness_index", "scarcity_index",
                              "validity_index", "completeness_index", "relevance_index", "accuracy_index")

    val normDF = MlHelplers.minMaxWrapper(transFeatures, df)

    // flatten normalized features to multiple columns
    val flatDF = normDF.select(
      col("asset_global_id") +: col("eval_date") +: col("asset_name") +:
        (0 until transFeatures.length).map(i => col("normFeatures")(i).alias(transFeatures(i))): _*
    )

    // get global config used to fit model
    val query = s"(select * from ${CONFIG_TABLE} ) global_config"
    val configDf = SourceHelplers.getDFfromJdbc(spark, query, dbConfigName)
    val bviWeight = configDf.filter(col("config_type").equalTo("bvi_weight")).select(col = CONFIG_COL)
      .first().getDecimal(0)
    val cviWeight = configDf.filter(col("config_type").equalTo("cvi_weight")).select(col = CONFIG_COL)
      .first().getDecimal(0)
    val iviWeight = configDf.filter(col("config_type").equalTo("ivi_weight")).select(col = CONFIG_COL)
      .first().getDecimal(0)

    // convert calc model to udf
    val bviUdf = udf(
      // calc and convert java.bigDecimal to Double
      (scarcityIndex:Double, relevanceIndex:Double, timelineIndex:Double) =>
          (scarcityIndex + relevanceIndex) * timelineIndex * bviWeight.doubleValue()
    )
    val cviUdf = udf(
      (costIndex:Double, timelineIndex:Double, validityIndex:Double) =>
          costIndex * timelineIndex * (1 - validityIndex) * cviWeight.doubleValue()
    )
    val iviUdf = udf(
      (completeIndex:Double, accuracyIndex:Double, stabilityIndex:Double, validityIndex:Double) =>
          (completeIndex + accuracyIndex + 1 - stabilityIndex) * (1 - validityIndex) * iviWeight.doubleValue()
    )

    flatDF
      .withColumn("bvi_index",
        bviUdf(col("scarcity_index"),col("relevance_index"), col("timeliness_index")))
      .withColumn("cvi_index",
        cviUdf(col("cost_index"),col("timeliness_index"), col("validity_index")))
      .withColumn("ivi_index",
        iviUdf(col("completeness_index"),col("accuracy_index"), col("stability_index"),
          col("validity_index")))
  }


  override def process(config: ConfigDefinition): Unit = {
    val sourceDF = SourceHelplers.getDFfromJdbc(spark, config.sourceName, dbConfigName)

    val etlDefinition = new EtlDefinition(
      sourceDF = sourceDF,
      transform = someTransform(),
      write = WriteHelplers.write2Jdbc(config.targetName, dbConfigName)
    )

    etlDefinition.process()
  }

}


