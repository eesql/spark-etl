package com.dtdream.ecd.dtvengeance.etl

import scala.reflect.runtime.universe
import com.typesafe.config.ConfigFactory

import collection.JavaConverters._


case class ConfigDefinition (
                              taskName: String,
                              sourceName: String,
                              sourceType: String,
                              targetName: String,
                              targetType: String,
                              sql:String = ""
                            )

case class MetaDefinition (config: ConfigDefinition, task: SparkTask)

object Routes {

  val config = ConfigFactory.load()

  // Scala 反射机制
  val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  // Config Root Name
  val name = config.getConfigList("root.tasks").asScala

  // 配置文件获取task列表
  val configs = name.map(f => ConfigDefinition(f.getString("task_name"),
    f.getString("source_name"),
    f.getString("source_type"),
    f.getString("target_name"),
    f.getString("target_type"),
    f.getString("sql"))
  )

  val nameList = configs.map(_.taskName)
  val metaList = configs.map( cfg => MetaDefinition(
    cfg,
    runtimeMirror.reflectModule(
      runtimeMirror.staticModule("com.dtdream.ecd.dtvengeance.tasks.%s".format(cfg.taskName))
    ).instance.asInstanceOf[SparkTask]
  ))

  val routes:Map[String, MetaDefinition] = (nameList zip metaList).toMap
}

