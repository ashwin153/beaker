package beaker.server

import beaker.server.Database._
import beaker.server.protobuf._

import java.io.Closeable
import scala.math.Ordering.Implicits._
import scala.util.{Failure, Try}

/**
 * A transactional key-value store.
 */
trait Database extends Closeable {

  /**
   * Returns the revision of the specified keys.
   *
   * @param keys Keys to read.
   * @return Revision of each key.
   */
  def read(keys: Set[Key]): Try[Map[Key, Revision]]

  /**
   * Applies the changes to the database.
   *
   * @param changes Changes to apply.
   * @return Whether or not changes were applied.
   */
  def write(changes: Map[Key, Revision]): Try[Unit]

  /**
   * Returns the revisions of the first limit keys after, but not including, the specified key.
   *
   * @param after Exclusive initial key.
   * @param limit Maximum number to return.
   * @return Revisions of keys in range.
   */
  def scan(after: Option[Key], limit: Int): Try[Map[Key, Revision]]

  /**
   * Attempts to commit the transaction on the database. Transactions may be committed if and only
   * if the versions they depend on are greater than or equal to their versions in the database.
   * Revisions are monotonic; if a transaction changes a key for which there exists a newer
   * revision, the modification is discarded. This ensures that transactions cannot undo the effect
   * of other transactions.
   *
   * @param transaction Transaction to commit.
   * @throws Conflicts If a newer version of a dependent key exists.
   * @return Whether or not the transaction was committed.
   */
  def commit(transaction: Transaction): Try[Unit] = {
    val (rset, wset) = (transaction.depends, transaction.changes)
    read(rset.keySet ++ wset.keySet) flatMap { values =>
      val latest  = values.withDefaultValue(Revision.defaultInstance)
      val invalid = rset collect { case (k, v) if v < latest(k).version => k -> latest(k) }
      val updates = wset collect { case (k, v) if v > latest(k) => k -> v }
      if (invalid.isEmpty) write(updates) else Failure(Database.Conflicts(invalid))
    }
  }

}

object Database {

  /**
   * A failure that indicates a subset of the dependencies of a transaction are invalid.
   *
   * @param invalid Latest revision of invalid dependencies.
   */
  case class Conflicts(invalid: Map[Key, Revision]) extends Exception

}
