package beaker.core
package storage

import beaker.core
import beaker.core.protobuf._

import pureconfig.loadConfigOrThrow
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
    database: Database,
    client: Jedis,
    expiration: Duration
  ) extends core.Cache {

    override def fetch(keys: Set[Key]): Try[Map[Key, Revision]] = Try {
      val seq = keys.toSeq
      seq.zip(this.client.mget(seq: _*).asScala)
        .collect { case (k, s) if s != null => k -> Revision.parseFrom(s.getBytes(Cache.Repr)) }
        .toMap
    }

    override def update(changes: Map[Key, Revision]): Try[Unit] = Try {
      val values = changes.mapValues(r => new String(r.toByteArray, Cache.Repr))
      values foreach { case (k, v) => this.client.setex(k, this.expiry.toSeconds.toInt, v) }
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
     * A [[Redis.Cache]] configuration.
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
     * Constructs a [[Redis.Cache]] by loading the configuration from the classpath.
     *
     * @param database Underlying database.
     * @return Classpath-configured [[Redis.Cache]].
     */
    def apply(database: Database): Redis.Cache =
      Redis.Cache(database, loadConfigOrThrow[Config]("beaker.cache.redis"))

    /**
     * Constructs a [[Redis.Cache]] from the provided configuration.
     *
     * @param database Underlying database.
     * @param config Configuration.
     * @return Dynamically-configured [[Redis.Cache]].
     */
    def apply(database: Database, config: Config): Redis.Cache =
      Redis.Cache(database, new Jedis(config.host, config.port), config.expiration)

  }

}
