package com.dtdream.ecd.dtvengeance.tasks

import com.dtdream.ecd.dtvengeance.etl.{ConfigDefinition, EtlDefinition, SparkTask}
import com.dtdream.ecd.dtvengeance.utils._
import org.apache.spark.sql.DataFrame


object AssetIndexTask extends SparkSessionWrapper with SparkTask {

  private val spark = getSession()
  private val dbConfigName = "jdbc-assets"

  private val CONFIG_TABLE = "t_asset_eval_global_config"
  private val CONFIG_COLUMN = "config_metric_min"


  def someTransform()(df: DataFrame): DataFrame = {

    val costDF = bucketIdxTransform(
      df, "relevance", "cost_result", "cost_index")
    val stabDF = bucketIdxTransform(
      costDF, "stability", "stability_result", "stability_index")
    val resultDF = bucketIdxTransform(
      stabDF, "timeliness", "last_update_period", "timeliness_index", -1)

    resultDF
      .withColumnRenamed("statis_date", "eval_date")
      .withColumnRenamed("relevance_result", "relevance_index")
      .withColumnRenamed("accuracy_result","accuracy_index")
      .withColumnRenamed("completeness_result", "completeness_index")
      .withColumnRenamed("etl_job_status", "validity_index")
      .withColumnRenamed("scarcity_score", "scarcity_index")
  }

  def bucketIdxTransform(df: DataFrame,
                      configName:String,
                      orgFeature:String,
                      newFeature:String,
                      smooth:Int=1): DataFrame = {

    val query = s"(select * from ${CONFIG_TABLE} where config_type='${configName}') global_config"
    val configDf = SourceHelplers.getDFfromJdbc(spark, query, dbConfigName)
    configDf.printSchema()


    // Bucketing using database config
    val bucket_list = configDf.select(CONFIG_COLUMN)
      .collect().map(_(0).asInstanceOf[Integer].toDouble).toList
    MlHelplers.bucketBinWrapper(bucket_list, orgFeature, newFeature, df, smooth)
  }

  override def process(config: ConfigDefinition): Unit = {
    //val writePath = config.targetName
    val spark = getSession()
    val sourceDF = SourceHelplers.getDFfromJdbc(spark, config.sourceName, dbConfigName)

    val etlDefinition = new EtlDefinition(
      sourceDF = sourceDF,
      transform = someTransform(),
      write = WriteHelplers.write2Jdbc(config.targetName, dbConfigName)
    )

    etlDefinition.process()
  }

}


