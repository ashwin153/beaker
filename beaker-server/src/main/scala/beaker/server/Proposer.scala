package beaker.server

import beaker.client.Cluster
import beaker.common.concurrent.Locking
import beaker.common.util._
import beaker.server.protobuf._
import beaker.server.storage.Local

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.math.Ordering.Implicits._
import scala.util.Try

/**
 * A consensus coordinator.
 *
 * @param id Globally-unique identifier.
 * @param acceptors Beaker Acceptors.
 * @param learners Beaker Learners.
 * @param backoff Backoff interval.
 */
case class Proposer(
  id: Long,
  acceptors: Cluster,
  learners: Cluster,
  backoff: Duration
) extends Locking {

  private[this] val round   : AtomicInteger         = new AtomicInteger(1)
  private[this] val current : AtomicReference[View] = new AtomicReference(View.defaultInstance)

  /**
   * Closes all acceptors and learners.
   */
  def close(): Unit = {
    this.acceptors.close()
    this.learners.close()
  }

  /**
   * Atomically returns the current view of the configuration.
   *
   * @return Current view.
   */
  def view: View = shared {
    this.current.get
  }

  /**
   * Atomically returns the latest configuration.
   *
   * @return Current configuration.
   */
  def configuration: Configuration = shared {
    this.current.get.configuration
  }

  /**
   * Atomically reconfigures the acceptors and learners.
   *
   * @param view Updated view.
   */
  def reconfigure(view: View): Unit = exclusive {
    if (this.current.get < view) {
      this.acceptors.update(view.configuration.acceptors)
      this.learners.update(view.configuration.learners)
      this.current.set(view)
    }
  }

  /**
   * Returns the next ballot.
   *
   * @return Next ballot.
   */
  def next(): Ballot = after(Ballot.defaultInstance)

  /**
   * Returns the next ballot after the specified ballot.
   *
   * @param ballot Initial ballot.
   * @return Next ballot.
   */
  def after(ballot: Ballot): Ballot = {
    val next = this.round.getAndUpdate(r => 1 + (r max ballot.round max this.view.ballot.round))
    Ballot(next, this.id)
  }

  /**
   * Coordinate consensus on a proposal. Uses a variation of Generalized Paxos that has several
   * desirable properties. First, non-conflicting transactions may be simultaneously learned.
   * Second, stale revisions are automatically repaired. Third, transactions may be consistently
   * committed as long as at least a majority is non-faulty.
   *
   * @see https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-33.pdf
   * @see https://www.datastax.com/dev/blog/lightweight-transactions-in-cassandra-2-0
   * @see https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf
   *
   * @param proposal Proposed proposal.
   * @return Indefinitely tries to reach agreement.
   */
  def consensus(proposal: Proposal): Try[Unit] = {
    // Prepare the proposal on a quorum of beakers.
    this.acceptors.quorum(_.prepare(proposal), this.configuration.quorum) flatMap { promises =>
      val promise = promises.reduce(_ merge _).copy(ballot = proposal.ballot)
      if (proposal <| promise) {
        // If there exists a newer promise, then reconfigure and retry.
        reconfigure(promise.view)
        consensus(proposal.copy(ballot = after(promise.ballot), view = proposal.view))
      } else if (!promise.matches(proposal)) {
        // If the promise does not match the proposal, then retry with the promise.
        consensus(promise.copy(ballot = after(promise.ballot)))
      } else {
        // Otherwise, get all keys in the promise from a quorum.
        val depends = promise.applies.flatMap(_.depends.keySet)
        val changes = promise.applies.flatMap(_.changes.keySet)

        this.acceptors.quorum(_.get(depends.toSet), this.configuration.quorum) map { replicas =>
          // Determine the latest and the oldest version of each key.
          val latest = replicas.reduce(_ maximum _)
          val oldest = replicas.reduce(_ minimum _)
          val snapshot = Local.Database(latest)

          // Discard all transactions in the promise that cannot be committed and repair all keys
          // that are read - but not written - by the transaction with different revisions.
          val applies = promise.applies.filter(snapshot.commit(_).isSuccess)
          val repairs = (latest -- changes) filter { case (k, r) => oldest(k) < r }
          promise.copy(applies = applies, repairs = proposal.repairs maximum repairs)
        } filter { updated =>
          // Filter proposal that contain transactions or repairs.
          updated.applies.nonEmpty || updated.repairs.nonEmpty
        } flatMap { updated =>
          // Asynchronously send the updated promise to a quorum of beakers and retry.
          this.acceptors.quorumAsync(_.accept(updated), this.configuration.quorum)
          Thread.sleep(backoff.toMillis)
          consensus(updated.copy(ballot = after(updated.ballot)))
        }
      }
    }
  }

}

object Proposer {

  /**
   * Constructs a proposer that is uniquely identified by the address.
   *
   * @param address Network location.
   * @param backoff Backoff interval.
   * @return Initialized proposer.
   */
  def apply(address: Address, backoff: Duration = 1 second): Proposer = {
    val ip = ByteBuffer.wrap(InetAddress.getByName(address.name).getAddress).getInt
    val id = (ip.toLong << 32) | (address.port & 0xffffffffL)
    new Proposer(id, Cluster.empty, Cluster.empty, backoff)
  }

}
