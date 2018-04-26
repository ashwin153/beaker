package beaker.server
package storage

import beaker.server
import beaker.server.protobuf._
import com.github.benmanes.caffeine.{cache => caffeine}

import pureconfig._

import java.util.concurrent.{ConcurrentSkipListMap, TimeUnit}
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.Try

object Local {

  /**
   * An in-memory, thread-safe key-value store.
   *
   * @param underlying Underlying map.
   */
  case class Database(
    underlying: ConcurrentSkipListMap[Key, Revision]
  ) extends server.Database {

    override def close(): Unit = this.underlying.clear()

    override def read(keys: Set[Key]): Try[Map[Key, Revision]] =
      Try(keys.map(k => k -> this.underlying.getOrDefault(k, Revision.defaultInstance)).toMap)

    override def write(changes: Map[Key, Revision]): Try[Unit] =
      Try(changes foreach { case (k, r) => this.underlying.put(k, r) })

    override def scan(after: Option[Key], limit: Int): Try[Map[Key, Revision]] = {
      val range = after match {
        case Some(k) => this.underlying.tailMap(k, false).entrySet()
        case None => this.underlying.entrySet()
      }

      Try(range.iterator().asScala.take(limit).map(x => x.getKey -> x.getValue).toMap)
    }

  }

  object Database {

    /**
     * An in-memory database configuration.
     */
    case class Config(

    )

    /**
     * Constructs an in-memory database from the classpath configuration.
     *
     * @return Statically-configured database.
     */
    def apply(): Local.Database =
      Local.Database(loadConfigOrThrow[Config]("beaker.databases.local"))

    /**
     * Constructs an in-memory database from the provided configuration.
     *
     * @param config Configuration.
     * @return Dynamically-configured database.
     */
    def apply(config: Database.Config): Local.Database =
      Local.Database(Map.empty[Key, Revision])

    /**
     * Constructs an in-memory database initialized with the specified key-value pairs.
     *
     * @param initial Initial contents.
     * @return Initialized database.
     */
    def apply(initial: Map[Key, Revision]): Local.Database = {
      val underlying = new ConcurrentSkipListMap[Key, Revision]()
      underlying.putAll(initial.asJava)
      Local.Database(underlying)
    }

  }

  /**
   * An in-memory, thread-safe Caffeine cache.
   *
   * @param database Underlying database.
   * @param underlying In-memory cache.
   */
  case class Cache(
    database: server.Database,
    underlying: caffeine.Cache[Key, Revision]
  ) extends server.Cache {

    override def close(): Unit = this.underlying.invalidateAll()

    override def fetch(keys: Set[Key]): Try[Map[Key, Revision]] =
      Try(this.underlying.getAllPresent(keys.asJava).asScala.toMap)

    override def update(changes: Map[Key, Revision]): Try[Unit] =
      Try(this.underlying.putAll(changes.asJava))

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
     * Constructs an in-memory cache from the classpath configuration.
     *
     * @param database Underlying database.
     * @return Statically-configured cache.
     */
    def apply(database: server.Database): Local.Cache =
      Local.Cache(database, loadConfigOrThrow[Config]("beaker.caches.local"))

    /**
     * Constructs an in-memory cache from the provided configuration.
     *
     * @param database Underlying database.
     * @param config Configuration.
     * @return Dynamically-configured cache.
     */
    def apply(database: server.Database, config: Config): Local.Cache =
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
