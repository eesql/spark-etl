package com.dtdream.ecd.dtvengeance.etl

trait SparkTask {
  def process(config: ConfigDefinition):Unit
}
