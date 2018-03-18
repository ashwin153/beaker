package beaker.client

import beaker.server.protobuf._

import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable

/**
 * A Beaker client.
 *
 * @param channel Underlying channel.
 */
class Client(channel: ManagedChannel) {

  private[this] val blocking = BeakerGrpc.blockingStub(this.channel)
  private[this] val async = BeakerGrpc.stub(this.channel)

  /**
   * Closes the underlying channel.
   */
  def close(): Unit = this.channel.shutdown()

  /**
   * Returns the revisions of the keys.
   *
   * @param keys Keys to retrieve.
   * @return Revisions of keys.
   */
  def get(keys: Set[Key]): Map[Key, Revision] =
    this.blocking.get(Keys(keys.toSeq)).contents

  /**
   * Returns the revision of the key.
   *
   * @param key Key to retrieve.
   * @return Revision of key.
   */
  def get(key: Key): Option[Revision] = get(Set(key)).get(key)

  /**
   * Conditionally applies the changes and returns their updated versions.
   *
   * @param changes Changes to apply.
   * @return Updated versions.
   */
  def put(changes: Map[Key, Value]): Option[Map[Key, Version]] = {
    val latest = get(changes.keySet).mapValues(_.version)
    val update = changes.keys map { k => k -> (latest.getOrElse(k, 0) + 1) }
    if (cas(latest, changes)) Some(update.toMap) else None
  }

  /**
   * Conditionally applies the change and returns the updated version.
   *
   * @param key Key to change.
   * @param value Change to apply.
   * @return Updated version.
   */
  def put(key: Key, value: Value): Option[Version] =
    put(Map(key -> value)).flatMap(_.get(key))

  /**
   * Conditionally applies the changes if the dependencies remain unchanged.
   *
   * @param depends Dependencies.
   * @param changes Changes to apply.
   * @return Whether or not the changes were applied.
   */
  def cas(depends: Map[Key, Version], changes: Map[Key, Value]): Boolean = {
    val rset = depends ++ changes.keySet.map(k => k -> depends.getOrElse(k, 0L))
    val wset = changes map { case (k, v) => k -> Revision(rset(k) + 1, v) }
    this.blocking.propose(Transaction(rset, wset)).successful
  }

  /**
   * Returns an inconsistent snapshot of the revisions of all keys.
   *
   * @return Snapshot of revisions.
   */
  def dump(): Map[Key, Revision] = {
    val latch = new CountDownLatch(1)
    var range = Range(limit = 10000)
    val value = mutable.Map.empty[Key, Revision]

    // Asynchronously scans the remote beaker and repairs the local beaker.
    val observer: StreamObserver[Range] = this.async.scan(new StreamObserver[Revisions] {
      override def onError(throwable: Throwable): Unit =
        observer.onNext(range)

      override def onCompleted(): Unit =
        latch.countDown()

      override def onNext(revisions: Revisions): Unit = {
        value ++= revisions.contents
        range = range.copy(after = revisions.contents.keys.max)
        observer.onNext(range)
      }
    })

    // Block until the local beaker is refreshed.
    observer.onNext(range)
    latch.await()
    value.toMap
  }

}

object Client {

  /**
   * Constructs a client connected to the specified address.
   *
   * @param address Network location.
   * @return Connected client.
   */
  def apply(address: Address): Client =
    Client(address.name, address.port)

  /**
   * Constructs a client connected to the specified host.
   *
   * @param name Hostname.
   * @param port Port number.
   * @return Connected client.
   */
  def apply(name: String, port: Int): Client =
    new Client(ManagedChannelBuilder.forAddress(name, port).build())

}
