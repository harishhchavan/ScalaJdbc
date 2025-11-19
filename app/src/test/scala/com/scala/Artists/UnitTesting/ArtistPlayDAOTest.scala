package com.scala.Artists.UnitTesting

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.scala.dao.ArtistPlayDAO
import java.sql.DriverManager

class ArtistPlayDAOTest extends AnyFlatSpec with Matchers {

  behavior of "ArtistPlayDAO with H2 (external schema)"

  it should "fetch rows successfully" in {

    val db = ArtistsTest.jdbcTypeSafeDatabaseSettings

    Class.forName(db.driver)
    val conn = DriverManager.getConnection(db.url, db.user, db.password)
    conn.setAutoCommit(false)

    val rows = ArtistPlayDAO.findByUserId(conn, 1)

    rows should not be empty
    rows.head.userId shouldBe 1

    conn.close()
  }

  it should "update rows successfully" in {

    val db = ArtistsTest.jdbcTypeSafeDatabaseSettings

    val conn = DriverManager.getConnection(db.url, db.user, db.password)
    conn.setAutoCommit(false)

    val updated = ArtistPlayDAO.updatePlayCount(conn, 1, "Radiohead", 999)
    updated shouldBe 1

    conn.close()
  }

  it should "delete rows successfully" in {

    val db = ArtistsTest.jdbcTypeSafeDatabaseSettings

    val conn = DriverManager.getConnection(db.url, db.user, db.password)
    conn.setAutoCommit(false)

    val deleted = ArtistPlayDAO.deleteByUserId(conn, 2)
    deleted should be >= 1

    conn.close()
  }
}
