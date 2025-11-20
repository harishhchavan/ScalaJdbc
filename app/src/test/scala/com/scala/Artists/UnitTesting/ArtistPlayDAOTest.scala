package com.scala.Artists.UnitTesting

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.DriverManager
import com.scala.dao.ArtistPlayDAO
import com.scala.model.ArtistPlay
import scala.jdk.CollectionConverters._

class ArtistPlayDAOTest extends AnyFlatSpec with Matchers {

  // H2 connection settings (same as test config)
  private val url = "jdbc:h2:mem:testdb;MODE=MSSQLServer;DB_CLOSE_DELAY=-1"
  private val user = "sa"
  private val password = ""

  private def withConnection[T](f: java.sql.Connection => T): T = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(url, user, password)
    conn.setAutoCommit(false)
    try f(conn) finally {
      conn.close()
    }
  }

  it should "fetch rows successfully" in {
    withConnection { conn =>
      // create table
      val stmt = conn.createStatement()
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS ArtistPlays (
          | user_id INT, rank INT, artist_name VARCHAR(255), playcount INT, mbid VARCHAR(255)
          |)""".stripMargin)
      stmt.close()

      // insert rows
      val ps = conn.prepareStatement("INSERT INTO ArtistPlays(user_id, rank, artist_name, playcount, mbid) VALUES (?, ?, ?, ?, ?)")
      ps.setInt(1, 1);
      ps.setInt(2, 1);
      ps.setString(3, "Crystal Castles");
      ps.setInt(4, 1034);
      ps.setString(5, "b157...")
      ps.executeUpdate()
      ps.setInt(1, 1);
      ps.setInt(2, 2);
      ps.setString(3, "Radiohead");
      ps.setInt(4, 972);
      ps.setString(5, "a74b...")
      ps.executeUpdate()
      ps.close()
      conn.commit()

      val rows = ArtistPlayDAO.findByUserId(conn, 1)
      rows should not be empty
      rows.head.userId shouldBe 1
    }
  }

  it should "update rows successfully" in {
    withConnection { conn =>
      // setup
      val stmt = conn.createStatement()
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS ArtistPlays (
          | user_id INT, rank INT, artist_name VARCHAR(255), playcount INT, mbid VARCHAR(255)
          |)""".stripMargin)
      stmt.close()

      val ps = conn.prepareStatement("INSERT INTO ArtistPlays(user_id, rank, artist_name, playcount, mbid) VALUES (?, ?, ?, ?, ?)")
      ps.setInt(1, 1);
      ps.setInt(2, 1);
      ps.setString(3, "Radiohead");
      ps.setInt(4, 972);
      ps.setString(5, "a74b...")
      ps.executeUpdate()
      ps.close()
      conn.commit()

      val updated = ArtistPlayDAO.updatePlayCount(conn, 1, "Radiohead", 999)
      updated shouldBe 2

      // verify
      val rows = ArtistPlayDAO.findByUserId(conn, 1)
      rows.find(_.artistName == "Radiohead").get.playCount shouldBe 999
    }
  }

  it should "delete rows successfully" in {
    withConnection { conn =>
      // setup
      val stmt = conn.createStatement()
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS ArtistPlays (
          | user_id INT, rank INT, artist_name VARCHAR(255), playcount INT, mbid VARCHAR(255)
          |)""".stripMargin)
      stmt.close()

      val ps = conn.prepareStatement("INSERT INTO ArtistPlays(user_id, rank, artist_name, playcount, mbid) VALUES (?, ?, ?, ?, ?)")
      ps.setInt(1, 2);
      ps.setInt(2, 1);
      ps.setString(3, "Paint It Black");
      ps.setInt(4, 423);
      ps.setString(5, "mbid-1")
      ps.executeUpdate()
      ps.close()
      conn.commit()

      val deleted = ArtistPlayDAO.deleteByUserId(conn, 2)
      deleted should be >= 1
    }
  }
}
