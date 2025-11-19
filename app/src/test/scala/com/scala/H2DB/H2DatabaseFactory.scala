package com.scala.H2DB

import com.typesafe.config.{Config, ConfigFactory}
import java.sql.Connection
import java.sql.DriverManager

object H2DatabaseFactory extends H2DatabaseSettings {

  val config: Config = ConfigFactory.load()

  val connection: Connection = {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection(
      dbUrl(config),
      dbUser(config),
      dbPassword(config)
    )
  }

  def initTestSchema(schemaFile: String = "artist_schema.sql"): Unit = {
    runSchemaFile(connection, schemaFile)
  }
}
