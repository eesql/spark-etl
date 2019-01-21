package com.dtdream.ecd.dtvengeance.etl

import com.typesafe.config.ConfigFactory
import collection.JavaConverters._
import org.scalatest.FunSpec

class ConfigSpec extends FunSpec {
  describe("Configuration Spec") {
    it("runs config test") {
      val config = ConfigFactory.load()
      val name = config.getConfigList("root.tasks").asScala

      val meta = name.map(f => ConfigDefinition(f.getString("task_name"),
        f.getString("source_name"),
        f.getString("source_type"),
        f.getString("target_name"),
        f.getString("target_type"),
        f.getString("sql"))
      )

      assert(meta(0).taskName == "WorldCupsTask")
    }
  }
}
