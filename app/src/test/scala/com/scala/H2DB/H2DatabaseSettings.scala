package com.scala.H2DB

import com.typesafe.config.Config

trait H2DatabaseSettings {

  def dbUrl(config: Config): String =
    config.getString("db.h2.url")

  def dbUser(config: Config): String =
    config.getString("db.h2.user")

  def dbPassword(config: Config): String =
    config.getString("db.h2.password")

  /** Loads an .sql file from test/resources and executes it */
  def runSchemaFile(conn: java.sql.Connection, fileName: String): Unit = {
    val sqlStream = getClass.getClassLoader.getResourceAsStream(fileName)
    require(sqlStream != null, s"Schema file not found: $fileName")

    val sql = scala.io.Source.fromInputStream(sqlStream).mkString
    val stmt = conn.createStatement()
    stmt.execute(sql)
    stmt.close()
  }
}
