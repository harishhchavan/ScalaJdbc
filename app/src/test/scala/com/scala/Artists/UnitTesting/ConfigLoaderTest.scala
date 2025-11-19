package com.scala.Artists.UnitTesting

import com.scala.config.{TypeSafeConfigLoader, TypeSafeDatabaseSettings}
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigLoaderTest extends AnyFlatSpec with Matchers {

  val config = ConfigFactory.load()

  behavior of "ConfigLoader"

  it should "load app config correctly" in {
    val loader = new TypeSafeConfigLoader(config.getConfig("app"))
    loader.csvPath shouldBe "artist.csv"
  }

  it should "load db config correctly" in {
    val loader = new TypeSafeDatabaseSettings(config.getConfig("db"))
    loader.url shouldBe "jdbc:h2:mem:testdb;MODE=MSSQLServer;DB_CLOSE_DELAY=-1"
    loader.driver shouldBe "org.h2.Driver"
    loader.user shouldBe "sa"
  }
}
