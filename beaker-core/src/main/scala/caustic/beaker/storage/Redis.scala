//package caustic.beaker
//package storage
//
//import akka.actor.ActorSystem
//import akka.util.ByteString
//import caustic.beaker
//import caustic.beaker.storage
//import pureconfig.loadConfigOrThrow
//import redis.{ByteStringDeserializer, ByteStringSerializer, RedisClient}
//import java.io._
//import redis.clients.jedis.Jedis
//import scala.collection.JavaConverters._
//import scala.concurrent.duration.Duration
//import scala.concurrent.{Await, ExecutionContext, Future}
//import scala.util.Try
//
///**
// *
// */
//object Redis {
//
//  /**
//   * A Redis-backed, cache. Thread-safe.
//   *
//   * @param database Underlying database.
//   * @param client Redis client.
//   */
//  case class Cache(
//    database: Database,
//    client: Jedis
//  ) extends beaker.Cache {
//
//    override def read(keys: Set[Key]): Try[Map[Key, Revision]] = {
//      //
//      val seq = keys.toSeq
//
//      this.client.mget(seq: _*).asScala
//
//
//        .map(Revision.apply)
//
//
//
//      Try(seq.zip(this.client.mget(seq: _*).asScala).toMap.filter(_._2 != null))
//    }
//
//    override def get(keys: Set[Key])(
//      implicit ec: ExecutionContext
//    ): Future[Map[Key, Revision]] = {
//      val seq = keys.toSeq
//      this.client.mget(seq: _*)
//        .map(values => seq.zip(values) collect { case (k, Some(r)) => k -> r })
//        .map(_.toMap)
//    }
//
//    override def put(changes: Map[Key, Revision])(
//      implicit ec: ExecutionContext
//    ): Future[Unit] =
//      this.client.mset(changes).map(_ => Unit)
//
//    override def invalidate(keys: Set[Key])(
//      implicit ec: ExecutionContext
//    ): Future[Unit] =
//      this.client.del(keys.toSeq: _*).map(_ => Unit)
//
//    override def close(): Unit = {
//      this.client.stop()
//      super.close()
//    }
//
//  }
//
//  object Cache {
//
//    // Redis Serializer.
//    implicit val serializer: ByteStringSerializer[Revision] = revision => {
//      val bytes = new ByteArrayOutputStream()
//      val stream = new ObjectOutputStream(bytes)
//      stream.writeObject(revision)
//      bytes.close()
//      ByteString(bytes.toByteArray)
//    }
//
//    // Redis Deserializer.
//    implicit val deserializer: ByteStringDeserializer[Revision] = repr => {
//      val bytes = new ByteArrayInputStream(repr.toArray)
//      val stream = new ObjectInputStream(bytes)
//      val result = stream.readObject().asInstanceOf[Revision]
//      bytes.close()
//      result
//    }
//
//    // Implicit Actor System.
//    implicit val system: ActorSystem = ActorSystem.create()
//    sys.addShutdownHook(this.system.terminate())
//
//    /**
//     * A RedisCache configuration.
//     *
//     * @param host Redis hostname.
//     * @param port Redis port number.
//     * @param password Optional password.
//     */
//    case class Config(
//      host: String,
//      port: Int,
//      password: Option[String]
//    )
//
//    /**
//     * Constructs a RedisCache by loading the configuration from the classpath.
//     *
//     * @param database Underlying database.
//     * @return Classpath-configured RedisCache.
//     */
//    def apply(database: Database): Redis.Cache =
//      Redis.Cache(database, loadConfigOrThrow[Config]("caustic.cache.redis"))
//
//    /**
//     * Constructs a RedisCache from the provided configuration.
//     *
//     * @param database Underlying database.
//     * @param config Configuration.
//     * @return Dynamically-configured RedisCache.
//     */
//    def apply(database: Database, config: Config): Redis.Cache =
//      Redis.Cache(database, RedisClient(config.host, config.port, config.password))
//
//  }
//
//}
