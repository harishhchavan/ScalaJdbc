package com.scala.dao

import com.scala.model.ArtistPlay
import com.scala.util.Logging
import com.github.tototoshi.csv.CSVWriter

import java.sql.{Connection, PreparedStatement, Statement}
import scala.collection.mutable.ListBuffer

object ArtistPlayDAO extends Logging {


  def insertBatch(connection: Connection, records: Iterator[ArtistPlay], batchSize: Int): Unit = {
    val insertSQL =
      """INSERT INTO ArtistPlays (user_id, rank, artist_name, playcount, mbid)
         VALUES (?, ?, ?, ?, ?)"""

    var preparedStatement: PreparedStatement = null
    var count = 0
    val startTime = System.nanoTime()

    logger.info(s"Begin inserting records with batch = $batchSize")

    try {
      preparedStatement = connection.prepareStatement(insertSQL)
      for (record <- records) {
        preparedStatement.setInt(1, record.userId)
        preparedStatement.setInt(2, record.rank)
        preparedStatement.setString(3, record.artistName)
        preparedStatement.setInt(4, record.playCount)
        preparedStatement.setString(5, record.mbid)

        preparedStatement.addBatch()
        count += 1

        if (count % batchSize == 0) {
          preparedStatement.executeBatch()
          connection.commit()
          preparedStatement.clearBatch()

          logger.info(s"Inserted $count records so far...")
        }
      }

      // flush remaining if any
      preparedStatement.executeBatch()
      connection.commit()
      val duration = (System.nanoTime() - startTime) / 1e9
      logger.info(f"Done. Inserted $count records in $duration%.2f seconds.")
    } catch {
      case ex: Throwable =>
        logger.error("Error during batch insert, attempting rollback", ex)
        try connection.rollback()
        catch {
          case _: Throwable =>
        }
        throw ex
    } finally {
      if (preparedStatement != null)
        try preparedStatement.close()
        catch {
          case _: Throwable => ()
        }
    }
  }

  // -------------Select All----------------------
  def selectAll(connection: Connection): Iterator[ArtistPlay] = {
    logger.info("Fetching all ArtistPlays rows...")
    val sql = "SELECT user_id, rank, artist_name, playcount, mbid FROM ArtistPlays"

    val statement: Statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    new Iterator[ArtistPlay] {
      // prefetch pattern
      private var hasMore: Boolean = resultSet.next()

      override def hasNext: Boolean = {
        if (!hasMore) {
          try statement.close()
          catch { case _: Throwable => () }
        }
        hasMore
      }

      override def next(): ArtistPlay = {
        if (!hasNext) throw new NoSuchElementException("No more rows")
        val row = ArtistPlay(
          resultSet.getInt("user_id"),
          resultSet.getInt("rank"),
          resultSet.getString("artist_name"),
          resultSet.getInt("playcount"),
          resultSet.getString("mbid")
        )
        // advance
        hasMore = resultSet.next()
        row
      }
    }
  }

  // -------------Select By Id------------------
  def findByUserId(connection: Connection, userId: Int): List[ArtistPlay] = {
    logger.info(s"Fetching records for userId = $userId")

    val sql =
      """SELECT user_id, rank, artist_name, playcount, mbid
       FROM ArtistPlays WHERE user_id = ?"""

    val ps = connection.prepareStatement(sql)
    ps.setInt(1, userId)

    val rs = ps.executeQuery()
    val buffer = ListBuffer[ArtistPlay]()

    while (rs.next()) {
      buffer += ArtistPlay(
        rs.getInt("user_id"),
        rs.getInt("rank"),
        rs.getString("artist_name"),
        rs.getInt("playcount"),
        rs.getString("mbid")
      )
    }

    ps.close()
    buffer.toList
  }

  // ------------Update----------------------------
  def updatePlayCount(
                       connection: Connection,
                       userId: Int,
                       artistName: String,
                       newPlayCount: Int
                     ): Int = {

    logger.info(s"Updating playcount for userId=$userId, artist=$artistName")

    val sql =
      """UPDATE ArtistPlays
       SET playcount = ?
       WHERE user_id = ? AND artist_name = ?"""

    val ps = connection.prepareStatement(sql)
    ps.setInt(1, newPlayCount)
    ps.setInt(2, userId)
    ps.setString(3, artistName)

    val rows = ps.executeUpdate()
    connection.commit()

    logger.info(s"Rows updated = $rows")
    ps.close()
    rows
  }

  // -----------------Delete--------------------
  def deleteByUserId(connection: Connection, userId: Int): Int = {
    logger.info(s"Deleting records for userId = $userId")

    val sql = "DELETE FROM ArtistPlays WHERE user_id = ?"

    val ps = connection.prepareStatement(sql)
    ps.setInt(1, userId)

    val rows = ps.executeUpdate()
    connection.commit()

    logger.info(s"Deleted rows = $rows")
    ps.close()
    rows
  }

  // ------------DB to CSV------------------
  def exportToCSV(connection: Connection, outputPath: String): Unit = {
    logger.info(s"Exporting ArtistPlays table to CSV: $outputPath")

    val writer = CSVWriter.open(new java.io.File(outputPath))
    writer.writeRow(List("user_id", "rank", "artist_name", "playcount", "mbid"))

    val records = selectAll(connection)
    for (r <- records) {
      writer.writeRow(
        List(r.userId, r.rank, r.artistName, r.playCount, r.mbid)
      )
    }

    writer.close()
    logger.info("CSV export completed.")
  }
}
