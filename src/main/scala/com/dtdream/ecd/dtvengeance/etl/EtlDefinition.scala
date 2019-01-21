package com.dtdream.ecd.dtvengeance.etl

import org.apache.spark.sql.DataFrame

case class EtlDefinition(
  sourceDF: DataFrame,
  transform: (DataFrame => DataFrame),
  write: (DataFrame => Unit)
) {

  def process(): Unit = {
    write(sourceDF.transform(transform))
  }

  def test(): Unit = {
    sourceDF.transform(transform)
  }
}