package com.scala.Artists.UnitTesting

import com.scala.config.{TypeSafeConfigLoader, TypeSafeDatabaseSettings}
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigLoaderTest extends AnyFlatSpec with Matchers {

  behavior of "ConfigLoader"

  it should "load app config from supplied test config" in {
    val testConf = ConfigFactory.parseString(
      """app {
        |  csvPath = "artist.csv"
        |  batchSize = 100
        |}
        |""".stripMargin)
    val loader = new TypeSafeConfigLoader(testConf.getConfig("app"))
    loader.csvPath shouldBe "artist.csv"
    loader.batchSize shouldBe 100
  }

  it should "load db config from supplied test config" in {
    val testDb = ConfigFactory.parseString(
      """db {
        |  url = "jdbc:h2:mem:testdb;MODE=MSSQLServer;DB_CLOSE_DELAY=-1"
        |  user = "sa"
        |  password = ""
        |  driver = "org.h2.Driver"
        |}
        |""".stripMargin)
    val dbLoader = new TypeSafeDatabaseSettings(testDb.getConfig("db"))
    dbLoader.url should startWith("jdbc:h2:mem:testdb")
    dbLoader.driver shouldBe "org.h2.Driver"
    dbLoader.user shouldBe "sa"
  }
}
