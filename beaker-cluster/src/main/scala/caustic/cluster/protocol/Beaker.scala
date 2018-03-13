package caustic.cluster
package protocol

import caustic.beaker.thrift
import caustic.beaker.thrift.Revision
import caustic.cluster

import scala.collection.JavaConverters._

/**
 * A Beaker implementation.
 */
object Beaker {

  type Key = String
  type Version = Long
  type Value = String

  /**
   * A Beaker service.
   */
  case object Service extends cluster.Service[Beaker.Client] {

    private val underlying = Thrift.Service(new thrift.Beaker.Client.Factory())

    override def connect(address: Address): Beaker.Client =
      Beaker.Client(this.underlying.connect(address))

    override def disconnect(client: Beaker.Client): Unit =
      this.underlying.disconnect(client.underlying)

  }

  /**
   * A Beaker client.
   *
   * @param underlying Underlying [[Thrift.Client]].
   */
  case class Client(underlying: Thrift.Client[thrift.Beaker.Client]) {

    /**
     * Returns the latest known [[Revision]] of each key.
     *
     * @param keys Keys to lookup.
     * @return Latest known [[Revision]] of each key.
     */
    def get(keys: Set[Key]): Map[Key, Revision] =
      this.underlying.connection.get(keys.asJava).asScala.toMap

    /**
     * Returns the latest known [[Revision]] of the key.
     *
     * @param key Key to lookup.
     * @return Latest known [[Revision]] of the key.
     */
    def get(key: Key): Option[Revision] = get(Set(key)).get(key)

    /**
     * Attempts to update the values of the keys and returns the latest versions if successful.
     *
     * @param changes Updated values.
     * @return Whether or not changes were applied.
     */
    def put(changes: Map[Key, Value]): Option[Map[Key, Version]] = {
      val latest = get(changes.keySet).mapValues(_.version)
      if (cas(latest, changes)) Some(latest.mapValues(_ + 1)) else None
    }

    /**
     * Attempts to update the value of the key and returns the latest version if successful.
     *
     * @param key Key to update.
     * @param value Updated value.
     * @return Latest version of the key.
     */
    def put(key: Key, value: Value): Option[Version] =
      put(Map(key -> value)).flatMap(_.get(key))

    /**
     * Conditionally applies the changes if and only if the dependencies remain unchanged. Returns
     * true if the changes were applied and false otherwise.
     *
     * @param depends Dependent versions.
     * @param changes Updated values.
     * @return Whether or not changes were applied.
     */
    def cas(depends: Map[Key, Version], changes: Map[Key, Value]): Boolean =
      this.underlying.connection.cas(depends.mapValues(long2Long).asJava, changes.asJava)

    /**
     * Reloads the latest values of the keys. Guarantees that every instance will store the latest
     * [[Revision]] of the keys until they are subsequently modified. Returns true if the refresh
     * was successful and false otherwise.
     *
     * @param keys Keys to refresh.
     * @return Whether or not refresh was successful.
     */
    def refresh(keys: Set[Key]): Boolean =
      cas(get(keys).mapValues(_.version), Map.empty)

  }

}
