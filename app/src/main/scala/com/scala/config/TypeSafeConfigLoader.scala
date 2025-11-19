package com.scala.config

import com.typesafe.config.Config

class TypeSafeConfigLoader(config: Config) extends ConfigLoader {

  override def csvPath: String = config.getString("csvPath")
  override def batchSize: Int = config.getInt("batchSize")
}
