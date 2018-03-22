package beaker.server
package storage

import beaker.server
import beaker.server.protobuf._

import pureconfig._
import redis.clients.jedis.Jedis

import java.nio.charset.Charset
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.Try

object Redis {

  /**
   * A Redis-backed, cache. Thread-safe.
   *
   * @param database Underlying [[Database]].
   * @param client Redis client.
   */
  case class Cache(
    database: server.Database,
    client: Jedis,
    expiration: Duration
  ) extends server.Cache {

    override def fetch(keys: Set[Key]): Try[Map[Key, Revision]] = Try {
      val seq = keys.toSeq
      seq.zip(this.client.mget(seq: _*).asScala)
        .collect { case (k, s) if s != null => k -> Revision.parseFrom(s.getBytes(Cache.Repr)) }
        .toMap
    }

    override def update(changes: Map[Key, Revision]): Try[Unit] = Try {
      val values = changes.mapValues(r => new String(r.toByteArray, Cache.Repr))
      values foreach { case (k, v) => this.client.setex(k, this.expiration.toSeconds.toInt, v) }
    }

    override def close(): Unit = {
      this.client.close()
      super.close()
    }

  }

  object Cache {

    // Default character representation.
    val Repr: Charset = Charset.forName("UTF-8")

    /**
     * A Redis cache configuration.
     *
     * @param host Redis hostname.
     * @param port Redis port number.
     * @param expiration Duration after which stale entries are evicted.
     */
    case class Config(
      host: String,
      port: Int,
      expiration: Duration
    )

    /**
     * Constructs a Redis cache from the classpath configuration.
     *
     * @param database Underlying database.
     * @return Statically-configured cache.
     */
    def apply(database: server.Database): Redis.Cache =
      Redis.Cache(database, loadConfigOrThrow[Config]("beaker.cache.redis"))

    /**
     * Constructs a Redis cache from the provided configuration.
     *
     * @param database Underlying database.
     * @param config Configuration.
     * @return Dynamically-configured cache.
     */
    def apply(database: server.Database, config: Config): Redis.Cache =
      Redis.Cache(database, new Jedis(config.host, config.port), config.expiration)

  }

}
