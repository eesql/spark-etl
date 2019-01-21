package com.dtdream.ecd.dtvengeance.etl

import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.{IntegerType, StringType}

class EtlDefinitionSpec
   extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {


  describe("process") {

    /**
    it("runs a full ETL process and writes out data to a folder") {

      val sourceDF = spark.createDF(
        List(
          ("bob", 14),
          ("liz", 20)
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val etlDefinition = new EtlDefinition(
        sourceDF = sourceDF,
        transform = EtlHelpers.someTransform(),
        write = EtlHelpers.someWriter()
      )

      etlDefinition.process()

    }
      */

  }

  describe("etl collection") {

    /**
    it("can run etls that are organized in a map") {

      val sourceDF = spark.createDF(
        List(
          ("bob", 14),
          ("liz", 20)
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val etlDefinition = new EtlDefinition(
        sourceDF = sourceDF,
        transform = EtlHelpers.someTransform(),
        write = EtlHelpers.someWriter()
      )

      val etls = scala.collection.mutable.Map[String, EtlDefinition]("example" -> etlDefinition)

      etls += ("ex2" -> etlDefinition)

      etls("example").process()

    }
      */

  }

}
