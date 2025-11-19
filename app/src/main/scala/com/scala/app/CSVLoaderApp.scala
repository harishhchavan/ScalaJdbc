package com.scala.app

import com.scala.config.{TypeSafeConfigLoader, TypeSafeDatabaseSettings}
import com.scala.dao.ArtistPlayDAO
import com.scala.db.DBConnection
import com.scala.util.Logging
import com.typesafe.config.{Config, ConfigFactory}

object CSVLoaderApp extends App with Logging {

  private val configLoad: Config = ConfigFactory.load()
  private val fileConfig = configLoad.getConfig("app")
  private val jdbcConfig = configLoad.getConfig("db")

  val typeSafeConfigLoader = new TypeSafeConfigLoader(fileConfig)
  val jdbcSettings = new TypeSafeDatabaseSettings(jdbcConfig)

  logger.info("CSV Loader started...")

  val connection = DBConnection.getConnection(jdbcSettings)

  try {

    val sampleRows = ArtistPlayDAO.findByUserId(connection, 1)
    logger.info(s"fetch test passed. Sample row: " + sampleRows.mkString(","))

    val updated = ArtistPlayDAO.updatePlayCount(connection, userId = 1, artistName = "Harish", newPlayCount = 999)
    logger.info(s"Updated test passed? = $updated")

    val deleted = ArtistPlayDAO.deleteByUserId(connection, 1)
    logger.info(s"Delete test passed? : $deleted")

    ArtistPlayDAO.exportToCSV(connection, "output.csv")
    logger.info("Export to CSV test passed.")

    logger.info("=== All Manual Tests Completed ===")

  } finally {
    try connection.close()
    catch {
      case ex: Throwable => logger.warn("Error closing DB connection", ex)
    }
    logger.info("Database connection closed.")
  }
}
