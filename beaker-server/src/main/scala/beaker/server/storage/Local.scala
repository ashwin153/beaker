package beaker.server
package storage

import beaker.common.concurrent.Locking
import beaker.server
import beaker.server.protobuf._
import com.github.benmanes.caffeine.{cache => caffeine}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.Try

object Local {

  /**
   * An in-memory, thread-safe key-value store.
   *
   * @param underlying Underlying map.
   */
  case class Database(
    underlying: mutable.SortedMap[Key, Revision]
  ) extends server.Database with Locking {

    override def read(keys: Set[Key]): Try[Map[Key, Revision]] = shared {
      Try(this.underlying.filterKeys(keys.contains).toMap)
    }

    override def write(changes: Map[Key, Revision]): Try[Unit] = exclusive {
      Try(this.underlying ++= changes)
    }

    override def scan(after: Option[Key], limit: Int): Try[Map[Key, Revision]] = shared {
      val range = after match {
        case Some(k) => this.underlying.iteratorFrom(k).drop(1)
        case None => this.underlying.iterator
      }

      Try(range.take(limit).toMap)
    }

    override def close(): Unit =
      this.underlying.clear()

  }

  object Database {

    /**
     * Constructs an in-memory database initialized with the specified items.
     *
     * @param items Initial contents.
     * @return Initialized database.
     */
    def apply(items: (Key, Revision)*): Local.Database =
      Local.Database(items.toMap)

    /**
     * Constructs an in-memory database initialized with the specified key-value pairs.
     *
     * @param initial Initial contents.
     * @return Initialized database.
     */
    def apply(initial: Map[Key, Revision]): Local.Database =
      Local.Database(mutable.SortedMap(initial.toSeq: _*))

  }

  /**
   * An in-memory, thread-safe Caffeine cache.
   *
   * @param database Underlying database.
   * @param underlying In-memory cache.
   */
  case class Cache(
    database: Database,
    underlying: caffeine.Cache[Key, Revision]
  ) extends server.Cache {

    override def fetch(keys: Set[Key]): Try[Map[Key, Revision]] =
      Try(this.underlying.getAllPresent(keys.asJava).asScala.toMap)

    override def update(changes: Map[Key, Revision]): Try[Unit] =
      Try(this.underlying.putAll(changes.asJava))

    override def close(): Unit =
      this.underlying.invalidateAll()

  }

  object Cache {

    /**
     * An in-memory cache configuration.
     *
     * @param capacity Maximum size in bytes.
     * @param expiration Duration after which stale entries are evicted.
     */
    case class Config(
      capacity: Long,
      expiration: Duration
    )

    /**
     * Constructs an in-memory cache from the provided configuration.
     *
     * @param database Underlying database.
     * @param config Configuration.
     * @return Configured cache.
     */
    def apply(database: Database, config: Config): Local.Cache =
      Local.Cache(database, caffeine.Caffeine.newBuilder()
        .weigher((k: Key, r: Revision) => sizeof(k) + sizeof(r))
        .maximumWeight(config.capacity)
        .expireAfterAccess(config.expiration.toMillis, TimeUnit.MILLISECONDS)
        .build[Key, Revision]())

    /**
     * Returns the approximate size of the key-revision pair. In-memory caches are typically bounded
     * in size by a maximum number of entries, after which an eviction protocol (eg. LRU) kicks in
     * to trim the size of the cache. However, this approach only works for homogenous-entry caches
     * (ie. fixed length), because otherwise the actual memory utilization of the cache would grow
     * proportionally with the total length of its contents. Instead, we may exploit empirical
     * results (see https://github.com/ashwin153/sandbox/tree/master/footprint) about the length of
     * cache entries to instead bound the size of the cache by its total memory utilization. This
     * will lead to far more predictable sizes for heterogenous-entry caches.
     *
     * @param x Object.
     * @return Approximate size in bytes.
     */
    def sizeof(x: Any): Int = x match {
      case x: String => 40 + math.ceil(x.length / 4.0).toInt * 8
      case x: Revision => 24 + sizeof(x.value)
    }
  }

}