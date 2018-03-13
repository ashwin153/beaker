package caustic.beaker
package storage

import caustic.beaker
import caustic.beaker.thrift.Revision

import com.mchange.v2.c3p0.{ComboPooledDataSource, PooledDataSource}

import java.sql.{Connection, SQLException}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object SQL {

  /**
   * A SQL key-value store.
   *
   * @param pool Underlying C3P0 connection pool.
   * @param dialect SQL implementation.
   */
  case class Database(
    pool: PooledDataSource,
    dialect: SQL.Dialect
  ) extends beaker.Database {

    // Verify that the table schema exists.
    val exists: Try[Unit] = perform(this.dialect.create)

    override def read(keys: Set[Key]): Try[Map[Key, Revision]] =
      if (keys.isEmpty) Success(Map.empty) else perform(this.dialect.select(_, keys))

    override def write(changes: Map[Key, Revision]): Try[Unit] =
      if (changes.isEmpty) Success(Map.empty) else perform(this.dialect.upsert(_, changes))

    override def close(): Unit =
      this.pool.close()

    /**
     * Performs the specified operation on the [[Connection]] and return the result.
     *
     * @param f JDBC operations.
     * @throws SQLException If invalid operations or the database is inaccessible.
     * @return Result of operations or an exception on failure.
     */
    def perform[R](f: Connection => R): Try[R] = {
      var con: Connection = null
      Try {
        con = this.pool.getConnection()
        con.setTransactionIsolation(Connection.TRANSACTION_NONE)
        con.setAutoCommit(false)
        val res = f(con)
        con.commit()
        con.close()
        res
      } recoverWith { case e: Exception if con != null =>
        con.rollback()
        con.close()
        Failure(e)
      }
    }

  }

  object Database {

    /**
     * A SQL database configuration.
     *
     * @param username Database username.
     * @param password Database password.
     * @param dialect [[SQL.Dialect]] flavor.
     * @param url JDBC connection url.
     */
    case class Config(
      username: String = "root",
      password: String = "",
      dialect: SQL.Dialect = Dialect.MySQL,
      url: String = "jdbc:mysql://localhost:3306/test?serverTimezone=UTC"
    )

    /**
     * Constructs a SQL database from the provided configuration.
     *
     * @param config Configuration.
     * @return Dynamically-configured SQL database.
     */
    def apply(config: Config): SQL.Database = {
      // Setup a C3P0 connection pool.
      val pool = new ComboPooledDataSource()
      pool.setUser(config.username)
      pool.setPassword(config.password)
      pool.setDriverClass(config.dialect.driver)
      pool.setJdbcUrl(config.url)

      // Construct the corresponding database.
      SQL.Database(pool, config.dialect)
    }

  }

  /**
   * A JDBC-compatible, SQL implementation.
   */
  sealed trait Dialect {

    /**
     * Returns the JDBC driver class.
     *
     * @return JDBC driver.
     */
    def driver: String

    /**
     * Creates the database table if and only if it doesn't already exist.
     *
     * @param con JDBC connection.
     */
    def create(con: Connection): Unit

    /**
     * Returns the revisions of the selected keys.
     *
     * @param con JDBC connection.
     * @param keys Keys to select.
     * @return Revisions of all specified keys.
     */
    def select(con: Connection, keys: Set[Key]): Map[Key, Revision]

    /**
     * Bulk upserts the provided changes into the database.
     *
     * @param con JDBC connection.
     * @param changes Changes to apply.
     */
    def upsert(con: Connection, changes: Map[Key, Revision]): Unit

  }

  object Dialect {

    /**
     * A MySQL implementation.
     *
     * @see https://www.mysql.com/
     */
    object MySQL extends SQL.Dialect {

      override val driver: String = "com.mysql.cj.jdbc.Driver"

      override def create(con: Connection): Unit = {
        // MySQL limits the size of the maximum key length to 767 bytes. Because it uses UTF-8
        // encoding, which requires up to 3 bytes per character, the key may be 255 characters long.
        // https://wildlyinaccurate.com/mysql-specified-key-was-too-long-max-key-length-is-767-bytes
        val sql =
          s""" CREATE TABLE IF NOT EXISTS `caustic` (
             |   `key` VARCHAR(255) NOT NULL,
             |   `version` BIGINT,
             |   `value` TEXT,
             |   PRIMARY KEY(`key`)
             |  )
         """.stripMargin

        val statement = con.createStatement()
        statement.execute(sql)
        statement.close()
      }

      override def select(con: Connection, keys: Set[Key]): Map[Key, Revision] = {
        // Benchmarks show that WHERE IN is sufficiently performant even for large numbers of values.
        // Alternative approaches might be to construct and join a temporary table or to issue separate
        // SELECT queries for each key, but both were shown to be equally or less performant.
        // https://stackoverflow.com/q/4514697/1447029
        val sql =
          s""" SELECT `key`, `version`, `value`
             | FROM `caustic`
             | WHERE `key` IN (${List.fill(keys.size)("?").mkString(",")})
         """.stripMargin

        // Execute the statement, and parse the returned ResultSet.
        val statement = con.prepareStatement(sql)
        keys.zipWithIndex foreach { case (k, i) => statement.setString(i + 1, k) }
        val resultSet = statement.executeQuery()

        val buffer = mutable.Buffer.empty[(Key, Revision)]
        while (resultSet.next()) {
          val key     = resultSet.getString("key")
          val version = resultSet.getLong("version")
          val value   = resultSet.getString("value")
          buffer     += key -> new Revision(version, value)
        }

        resultSet.close()
        statement.close()
        buffer.toMap
      }

      override def upsert(con: Connection, changes: Map[Key, Revision]): Unit = {
        // Benchmarks show that this query performs significantly better than a REPLACE INTO and
        // equivalently to an INSERT in the absence of conflicts.
        val sql =
          s""" INSERT INTO `caustic` (`key`, `version`, `value`)
             | VALUES ${Seq.fill(changes.size)("(?, ?, ?)").mkString(",")}
             | ON DUPLICATE KEY UPDATE
             | `version` = VALUES(`version`),
             | `value` = VALUES(`value`)
         """.stripMargin

        // Execute the statement and return.
        val statement = con.prepareStatement(sql)
        changes.zipWithIndex foreach { case ((k, r), i) =>
          val row = i * 3
          statement.setString(row + 1, k)
          statement.setLong(row + 2, r.version)
          statement.setString(row + 3, r.value)
        }

        statement.executeUpdate()
        statement.close()
      }

    }

    /**
     * A PostgreSQL implementation.
     *
     * @see https://www.postgresql.org/
     */
    object PostgreSQL extends SQL.Dialect {

      override val driver: String = "org.postgresql.Driver"

      override def create(connection: Connection): Unit = {
        // Unlike MySQL, PostgreSQL has no defined limits on the size of keys. Keys are theoretically
        // bounded by the maximum query size (1 GB), but in practice this is almost never a concern.
        val sql =
          s""" CREATE TABLE IF NOT EXISTS caustic (
             |   key VARCHAR NOT NULL,
             |   version BIGINT DEFAULT 0,
             |   value TEXT,
             |   PRIMARY KEY(key)
             | )
          """.stripMargin

        val statement = connection.createStatement()
        statement.execute(sql)
        statement.close()
      }

      override def select(con: Connection, keys: Set[Key]): Map[Key, Revision] = {
        // Benchmarks show that WHERE IN is sufficiently performant even for large numbers of values.
        // Alternative approaches might be to construct and join a temporary table or to issue
        // separate SELECT queries for each key, but both were shown to be equally or less
        // performant. https://stackoverflow.com/q/4514697/1447029
        val sql =
        s""" SELECT key, version, value
           | FROM caustic
           | WHERE key IN (${List.fill(keys.size)("?").mkString(",")})
         """.stripMargin

        // Execute the statement, and parse the returned ResultSet.
        val statement = con.prepareStatement(sql)
        keys.zipWithIndex foreach { case (k, i) => statement.setString(i + 1, k) }
        val resultSet = statement.executeQuery()

        val buffer = mutable.Buffer.empty[(Key, Revision)]
        while (resultSet.next()) {
          val key     = resultSet.getString("key")
          val version = resultSet.getLong("version")
          val value   = resultSet.getString("value")
          buffer     += key -> new Revision(version, value)
        }

        resultSet.close()
        statement.close()
        buffer.toMap
      }

      override def upsert(con: Connection, changes: Map[Key, Revision]): Unit = {
        // Requires PostgreSQL 9.5+, but performs a bulk upsert that is similar in functionality to
        // INSERT ... ON DUPLICATE KEY UPDATE in MySQL. https://stackoverflow.com/a/34529505/1447029
        val sql =
          s""" INSERT INTO caustic (key, version, value)
             | VALUES ${Seq.fill(changes.size)("(?, ?, ?)").mkString(",")}
             | ON CONFLICT (key) DO UPDATE SET
             | version = excluded.version,
             | value = excluded.value
          """.stripMargin

        // Execute the statement and return.
        val statement = con.prepareStatement(sql)
        changes.zipWithIndex foreach { case ((k, r), i) =>
          val row = i * 4
          statement.setString(row + 1, k)
          statement.setLong(row + 2, r.version)
          statement.setString(row + 3, r.value)
        }

        statement.executeUpdate()
        statement.close()
      }

    }

  }

}
