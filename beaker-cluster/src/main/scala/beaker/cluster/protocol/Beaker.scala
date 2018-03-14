package beaker.cluster
package protocol

import beaker.core.protobuf._
import beaker.cluster
import beaker.core.protobuf.BeakerGrpc
import io.grpc.{Channel, ManagedChannel, ManagedChannelBuilder}
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

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

    override def connect(address: Address): Beaker.Client =
      Beaker.Client(ManagedChannelBuilder.forAddress(address.host, address.port).build())

    override def disconnect(client: Beaker.Client): Unit =
      client.channel.shutdown()

  }

  /**
   * A Beaker client.
   *
   * @param channel Underlying [[Channel]].
   */
  case class Client(channel: ManagedChannel) {

    private[this] val underlying = BeakerGrpc.blockingStub(this.channel)

    /**
     * Returns the latest known [[Revision]] of each key.
     *
     * @param keys Keys to lookup.
     * @return Latest known [[Revision]] of each key.
     */
    def get(keys: Set[Key]): Map[Key, Revision] =
      this.underlying.get(Keys(keys.toSeq)).entries

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
     * true if the changes were applied and false otherwise. Changes implicitly depend on the
     * initial version if no dependency is specified.
     *
     * @param depends Dependent versions.
     * @param changes Updated values.
     * @return Whether or not changes were applied.
     */
    def cas(depends: Map[Key, Version], changes: Map[Key, Value]): Boolean = {
      val rset = depends ++ changes.keySet.map(k => k -> depends.getOrElse(k, 0L))
      val wset = changes map { case (k, v) => k -> Revision(rset(k) + 1, v) }
      this.underlying.propose(Transaction(rset, wset)).successful
    }

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
