package com.scala.service

import com.scala.model.ArtistPlay
import com.scala.util.Logging
import com.github.tototoshi.csv.CSVReader

import java.io.File
class CSVReaderService extends Logging {

  def readArtistPlays(filePath: String): Iterator[ArtistPlay] = {

    logger.info(s"Reading CSV file: $filePath")

    val reader = CSVReader.open(new File(filePath))
    val iter = reader.iteratorWithHeaders

    val records = iter.flatMap { row =>
      try {
        Some(ArtistPlay(
          userId = row("user_id").toInt,
          rank = row("rank").toInt,
          artistName = row("artist_name"),
          playCount = row("playcount").toInt,
          mbid = row.getOrElse("mbid", "")
        ))
      } catch {
        case ex: Exception =>
          logger.warn(s"Skipping bad record: $row - ${ex.getMessage}")
          None
      }
    }

    new Iterator[ArtistPlay] {
      override def hasNext: Boolean = {
        val more = records.hasNext
        if (!more) {
          try reader.close()
          catch { case _: Throwable => () }
        }
        more
      }
      override def next(): ArtistPlay = records.next()
    }
  }
}
