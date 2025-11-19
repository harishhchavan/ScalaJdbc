package com.scala.db

import com.scala.config.DatabaseSettings
import java.sql.{Connection, DriverManager}

object DBConnection {

  def getConnection(settings: DatabaseSettings): Connection = {
    Class.forName(settings.driver)
    val conn = DriverManager.getConnection(settings.url, settings.user, settings.password)
    conn.setAutoCommit(false)
    conn
  }

  def getConnection(): Connection =
    throw new IllegalStateException("Please call getConnection(settings)")
}
