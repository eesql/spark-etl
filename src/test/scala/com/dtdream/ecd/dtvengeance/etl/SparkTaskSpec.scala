package com.dtdream.ecd.dtvengeance.etl

import com.dtdream.ecd.dtvengeance.tasks._
import org.scalatest.FunSpec

class SparkTaskSpec extends FunSpec{
  describe("spark task test") {

    /**
    it("test WorldCupsTask") {
      val meta = MetaDefinition(ConfigDefinition("WorldCupsTask",
        "./src/main/resources/WorldCups.csv",
        "jdbc-assets",
        "test_min_max",
        "mysql"),WorldCupsTask)
      meta.task.process(meta.config)
      assert(true == true)
    }*/


    /**
    it("test AssetIndexTask") {
      val meta = MetaDefinition(ConfigDefinition("AssetIndexTask",
        "t_asset_statis_result_day",
        "jdbc-assets",
        "test_jdbc_index",
        "mysql"),AssetIndexTask)
      meta.task.process(meta.config)
      assert(true == true)
    }*/


    it("test HiveTableSyncTask") {
      val meta = MetaDefinition(ConfigDefinition("HiveTableSyncTask",
        "t_punish_info_7",
        "jdbc-legal",
        "ods_t_punish_info_7",
        "hive"),HiveTableSyncTask)
      meta.task.process(meta.config)
      assert(true == true)
    }

  }
}
