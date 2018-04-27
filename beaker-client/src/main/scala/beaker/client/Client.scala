package beaker.client

import beaker.common.util._
import beaker.server.protobuf._
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel, ManagedChannelBuilder}
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * A Beaker client.
 *
 * @param channel Underlying channel.
 */
class Client(channel: ManagedChannel) {

  /**
   * Conditionally applies the changes if the dependencies remain unchanged.
   *
   * @param depends Dependencies.
   * @param changes Changes to apply.
   * @return Updated versions.
   */
  def cas(depends: Map[Key, Version], changes: Map[Key, Value]): Try[Map[Key, Version]] = {
    val rset = depends ++ (changes.keySet -- depends.keySet).map(k => k -> 0L)
    val wset = changes map { case (k, v) => k -> Revision(rset(k) + 1, v) }

    Try(BeakerGrpc.blockingStub(this.channel)
      .withDeadlineAfter(50, TimeUnit.MILLISECONDS)
      .propose(Transaction(rset, wset)))
      .filter(_.successful)
      .map(_ => wset.mapValues(_.version))
  }

  /**
   * Conditionally applies the changes if the dependencies remain unchanged.
   *
   * @param depends Dependencies.
   * @param changes Changes to apply.
   * @throws Exception If changes could not be applied.
   * @return Updated versions.
   */
  def cas(depends: util.Map[Key, Version], changes: util.Map[Key, Value]): Try[util.Map[Key, Version]] =
    cas(depends.asScala, changes.asScala).map(_.asJava)

  /**
   * Closes the underlying channel.
   */
  def close(): Unit = this.channel.shutdown()

  /**
   * Returns the revision of the key.
   *
   * @param key Key to retrieve.
   * @return Revision of key.
   */
  def get(key: Key): Try[Option[Revision]] = get(Set(key)).map(_.get(key))

  /**
   * Returns the revisions of the keys.
   *
   * @param keys Keys to retrieve.
   * @return Revisions of keys.
   */
  def get(keys: Iterable[Key]): Try[Map[Key, Revision]] =
    Try(BeakerGrpc.blockingStub(this.channel)
      .withDeadlineAfter(50, TimeUnit.MILLISECONDS)
      .get(Keys(keys.toSeq)).entries)

  /**
   * Returns the revisions of the keys.
   *
   * @param keys Keys to retrieve.
   * @return Revisions of keys.
   */
  def get(keys: util.Collection[Key]): Try[util.Map[Key, Revision]] =
    get(keys.asScala).map(_.asJava)

  /**
   * Asynchronously applies the function to every key.
   *
   * @param f Function to apply.
   * @param by Chunk size.
   * @return Asynchronous result.
   */
  def foreach[U](f: (Key, Revision) => U, by: Int = 10000): Future[Unit] =
    scan(_.foreach(f.tupled), by = by)

  /**
   * Returns the current view of the network configuration.
   *
   * @return Network configuration.
   */
  def network(): Try[View] =
    Try(BeakerGrpc.blockingStub(this.channel)
      .withDeadlineAfter(50, TimeUnit.MILLISECONDS)
      .network(Void()))

  /**
   * Conditionally applies the change and returns the updated version.
   *
   * @param key Key to change.
   * @param value Change to apply.
   * @return Updated version.
   */
  def put(key: Key, value: Value): Try[Version] =
    put(Map(key -> value)).flatMap(_.get(key).toTry)

  /**
   * Conditionally applies the changes and returns their updated versions.
   *
   * @param changes Changes to apply.
   * @return Updated versions.
   */
  def put(changes: Map[Key, Value]): Try[Map[Key, Version]] =
    get(changes.keySet).map(_.mapValues(_.version)).flatMap(cas(_, changes))

  /**
   * Conditionally applies the changes and returns their updated versions.
   *
   * @param changes Changes to apply.
   * @return Updated versions.
   */
  def put(changes: util.Map[Key, Value]): Try[util.Map[Key, Version]] =
    put(changes.asScala).map(_.asJava)

  /**
   * Attempts to consistently update the network configuration.
   *
   * @param configuration Updated configuration.
   * @return Whether or not the reconfiguration was successful.
   */
  def reconfigure(configuration: Configuration): Try[Unit] =
    Try(BeakerGrpc.blockingStub(this.channel)
      .withDeadlineAfter(50, TimeUnit.MILLISECONDS)
      .reconfigure(configuration))
      .filter(_.successful)

  /**
   * Asynchronously applies the function to all keys in the specified range in chunks of the
   * specified size.
   *
   * @param f Function to apply.
   * @param from Inclusive initial key.
   * @param to Exclusive terminal key.
   * @param by Chunk size.
   * @return Asynchronous result.
   */
  def scan[U](
    f: Map[Key, Revision] => U,
    from: Option[Key] = None,
    to: Option[Key] = None,
    by: Int = 10000
  ): Future[Unit] = {
    val promise = Promise[Unit]()
    var range = Range(from, by)
    val server = new AtomicReference[StreamObserver[Range]]

    // Asynchronously scans the remote beaker and repairs the local beaker.
    val observer = BeakerGrpc.stub(this.channel).scan(new StreamObserver[Revisions] {
      override def onError(throwable: Throwable): Unit =
        promise.failure(throwable)

      override def onCompleted(): Unit =
        promise.success(())

      override def onNext(revisions: Revisions): Unit = {
        val entries = revisions.entries.filterKeys(k => !to.exists(_ <= k))
        f(entries)

        if (entries.size < by) {
          server.get().onCompleted()
        } else {
          range = range.withAfter(entries.keys.max)
          server.get().onNext(range)
        }
      }
    })

    // Block until the local beaker is refreshed.
    server.set(observer)
    observer.onNext(range)
    promise.future
  }

  /**
   * Requests a promise for a proposal.
   *
   * @param proposal Proposal to prepare.
   * @return Promise.
   */
  private[beaker] def prepare(proposal: Proposal): Try[Proposal] =
    Try(BeakerGrpc.blockingStub(this.channel)
      .withDeadlineAfter(50, TimeUnit.MILLISECONDS)
      .prepare(proposal))

  /**
   * Requests a vote for a proposal.
   *
   * @param proposal Proposal to vote for.
   * @return Whether or not a vote was cast.
   */
  private[beaker] def accept(proposal: Proposal): Future[Unit] = {
    val fork = Context.current().fork()
    val prev = fork.attach()

    try {
      BeakerGrpc.stub(this.channel)
        .withDeadlineAfter(50, TimeUnit.MILLISECONDS)
        .accept(proposal)
        .filter(_.successful)
    } finally {
      fork.detach(prev)
    }
  }

  /**
   * Casts a vote for a proposal.
   *
   * @param proposal Proposal to commit.
   * @return Whether or not a vote was successfully cast.
   */
  private[beaker] def learn(proposal: Proposal): Future[Unit] = {
    val fork = Context.current().fork()
    val prev = fork.attach()

    try {
      BeakerGrpc.stub(this.channel)
        .withDeadlineAfter(50, TimeUnit.MILLISECONDS)
        .learn(proposal)
    } finally {
      fork.detach(prev)
    }
  }

}

object Client {

  /**
   * Constructs a client connected to the specified url.
   *
   * @param url Connect string. ($host:$port)
   * @return Connected client.
   */
  def apply(url: String): Client =
    Client(url.split(":").head, url.split(":").last.toInt)

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
    new Client(ManagedChannelBuilder.forAddress(name, port).usePlaintext(true).build())

}
