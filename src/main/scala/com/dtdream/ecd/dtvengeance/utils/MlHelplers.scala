package com.dtdream.ecd.dtvengeance.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.ml.feature.{Bucketizer, MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._


object MlHelplers extends LazyLogging {

  def bucketBinWrapper(splitConfig:List[Double],
                       orgFeature:String,
                       newFeatureName:String,
                       df:DataFrame,
                       smooth:Int=1):DataFrame = {

    val splits = Double.NegativeInfinity +: splitConfig.toArray :+ Double.PositiveInfinity

    val bucketizer = new Bucketizer()
      .setInputCol(orgFeature)
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(df)

    /** Config which column to drop depending on whether smoothing is needed.
      * 1 stands for smoothing while -1 stands for not smoothing
    */
    val colMap = Map(1 -> "bucketedFeatures", -1 -> "bucketSmoothFeature")

    /** Smoothing bucketed features using given aggregate function */
    // TODO: add more aggregate methods for smoothing
    val bucketedMin = bucketedData.groupBy("bucketedFeatures")
      .mean(orgFeature).withColumnRenamed(s"avg(${orgFeature})", "bucketSmoothFeature")

    /** Join and return bucketed feature
        using array type to prevent duplicate columns when joining
      */
    bucketedData.drop(orgFeature)
      .join(bucketedMin, Seq("bucketedFeatures"))
      .drop(colMap.get(smooth).get)
      .withColumnRenamed(colMap.get(-1*smooth).get, newFeatureName)
  }

  def minMaxWrapper(featureList:Array[String], df:DataFrame):DataFrame = {

    /** Assemble features into vector so that we can transform multiple columns at one time
      */
    val assembler = new VectorAssembler()
      .setInputCols(featureList)
      .setOutputCol("features")

    val assembledDF = assembler.transform(df)

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(assembledDF)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(assembledDF)
    logger.info(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")

    // transform type of features from vector to array
    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    val toArrUdf = udf(toArr)
    scaledData.withColumn("normFeatures",toArrUdf(col("scaledFeatures")))
  }
}
