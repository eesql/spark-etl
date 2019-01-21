package com.dtdream.ecd.dtvengeance.utils

import org.apache.spark.sql.{DataFrame, SaveMode}

object WriteHadoopHelplers extends SparkSessionWrapper {



  def write2Hive(outputTableName:String)(df:DataFrame):Unit = {
    //val spark = this.getSessionWithHive()
    df.write.mode(SaveMode.Overwrite).saveAsTable(outputTableName)
  }
}
