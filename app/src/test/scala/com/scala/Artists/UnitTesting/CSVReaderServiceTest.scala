package com.scala.Artists.UnitTesting

import com.scala.service.CSVReaderService
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CSVReaderServiceTest extends AnyFlatSpec with Matchers {

  behavior of "CSVReaderService"

  it should "parse all rows from test_valid.csv" in {
    val filePath = getClass.getResource("/test_valid.csv").getPath
    val csvReader = new CSVReaderService()

    val records = csvReader.readArtistPlays(filePath).toList

    records should have size 3
    records.head.artistName shouldBe "Crystal Castles"
  }

  it should "skip or handle invalid rows in test_invalid.csv" in {
    val filePath = getClass.getResource("/test_invalid.csv").getPath
    val csvReader = new CSVReaderService()

    val rows = csvReader.readArtistPlays(filePath).toList

    rows.nonEmpty shouldBe true

    noException should be thrownBy csvReader.readArtistPlays(filePath).toList
  }

  it should "set mbid empty when missing in valid CSV" in {
    val filePath = getClass.getResource("/test_valid.csv").getPath
    val csvReader = new CSVReaderService()
    val rows = csvReader.readArtistPlays(filePath).toList

    val afiRow = rows.find(_.artistName == "AFI")
    afiRow.foreach(_.mbid shouldBe "")
  }
}
