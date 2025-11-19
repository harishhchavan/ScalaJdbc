package com.scala.Artists.UnitTesting

import com.scala.config.{TypeSafeConfigLoader, TypeSafeDatabaseSettings}
import com.typesafe.config.{Config, ConfigFactory}

object ArtistsTest {

  private val configLoad: Config = ConfigFactory.load()

  val fileConfig = configLoad.getConfig("app")
  val jdbcConfig = configLoad.getConfig("db")

  val typeSafeConfigLoader = new TypeSafeConfigLoader(fileConfig)
  val jdbcTypeSafeDatabaseSettings = new TypeSafeDatabaseSettings(jdbcConfig)
}
