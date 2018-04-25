package beaker.server

import beaker.common.util._
import beaker.server.protobuf._

import scala.util.{Failure, Success, Try}

/**
 * A transactional, write-through cache.
 */
trait Cache extends Database {

  /**
   * Returns the underlying database.
   *
   * @return Underlying database.
   */
  def database: Database

  /**
   * Returns the cached revision of the specified keys.
   *
   * @param keys Keys to fetch.
   * @return Cached revision of the keys.
   */
  def fetch(keys: Set[Key]): Try[Map[Key, Revision]]

  /**
   * Applies the changes to the cache.
   *
   * @param changes Changes to apply.
   * @return Whether or not the update was successful.
   */
  def update(changes: Map[Key, Revision]): Try[Unit]

  override def close(): Unit = {
    // Propagate close to the underlying database.
    this.database.close()
  }

  override def commit(transaction: Transaction): Try[Unit] = {
    this.database.commit(transaction) match {
      case Success(_) =>
        // Update the values of changed keys.
        update(transaction.changes)
      case Failure(Database.Conflicts(invalid)) =>
        // Update the invalid keys and propagate failures.
        update(invalid)
        Failure(Database.Conflicts(invalid))
      case Failure(e) =>
        // Propagate all other failures.
        Failure(e)
    }
  }

  override def read(keys: Set[Key]): Try[Map[Key, Revision]] = {
    fetch(keys) recover { case _ => Map.empty[Key, Revision] } flatMap { hits =>
      val misses = hits.keySet diff keys
      if (misses.nonEmpty) {
        // If any keys missed cache, reload them from the database.
        this.database.read(misses) map { changes =>
          update(changes)
          hits ++ changes
        }
      } else {
        // Otherwise, return the cache hits.
        Success(hits)
      }
    }
  }

  override def scan(after: Option[Key], limit: Int): Try[Map[Key, Revision]] = {
    // Scan from the underlying database and avoid cache entirely.
    this.database.scan(after, limit)
  }

  override def write(changes: Map[Key, Revision]): Try[Unit] = {
    // Write to the database and then update the cache.
    this.database.write(changes).andThen(_ => this.update(changes))
  }

}