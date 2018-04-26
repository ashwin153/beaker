package beaker.server

import beaker.client.Cluster
import beaker.common.concurrent.Locking
import beaker.common.util._
import beaker.server.protobuf._
import beaker.server.storage.Local

import com.typesafe.scalalogging.LazyLogging

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
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
) extends Locking with LazyLogging {

  private[this] val round: AtomicLong = new AtomicLong(1)
  private[this] val current: AtomicReference[View] = new AtomicReference(View.defaultInstance)

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
   * Closes all acceptors and learners.
   */
  def close(): Unit = {
    this.acceptors.close()
    this.learners.close()
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
    this.logger.debug("Preparing {}", proposal.commits.hashCode())
    this.acceptors.quorum(_.prepare(proposal)) flatMap { promises =>
      val promise = promises.reduce(_ merge _)
      if (proposal.ballot < promise.ballot || proposal.view < promise.view) {
        // If there exists a newer promise, then reconfigure and retry.
        reconfigure(promise.view)
        consensus(proposal.copy(ballot = after(promise.ballot)))
      } else if (!promise.equivalent(proposal)) {
        // If the promise does not match the proposal, then retry with the promise.
        reconfigure(promise.view)
        consensus(promise.copy(ballot = after(promise.ballot)))
      } else {
        // Otherwise, get all keys in the proposal from a quorum.
        this.logger.debug("Reading {}", proposal.commits.hashCode())
        val depends = proposal.commits.flatMap(_.depends.keySet)
        this.acceptors.quorum(_.get(depends.toSet)) map { replicas =>
          // Determine the latest and the oldest version of each key.
          val latest = replicas.reduce(_ maximum _)
          val oldest = replicas.reduce(_ minimum _).withDefaultValue(Revision.defaultInstance)
          val snapshot = Local.Database(latest)

          // Discard all transactions in the proposal that cannot be committed and repair all keys
          // that are read - but not written - by the proposal with different revisions.
          val commits = proposal.commits.filter(snapshot.commit(_).isSuccess)
          val changes = commits.flatMap(_.changes.keySet)
          val repairs = (latest -- changes) filter { case (k, r) => oldest(k) < r }
          proposal.copy(commits = commits, repairs = proposal.repairs maximum repairs)
        } filter { updated =>
          // Filter proposal that contain transactions or repairs or a new view.
          updated.commits.nonEmpty || updated.repairs.nonEmpty || updated.view > this.view
        } flatMap { updated =>
          // Asynchronously send the updated proposal to a quorum of beakers and retry.
          this.logger.debug("Accepting {}", proposal.commits.hashCode())
          this.acceptors.broadcastAsync(_.accept(updated))
          Thread.sleep(backoff.toMillis)
          consensus(updated.copy(ballot = after(updated.ballot)))
        }
      }
    }
  }

  /**
   * Returns the next ballot.
   *
   * @return Next ballot.
   */
  def next(): Ballot = after(Ballot.defaultInstance)

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
   * Atomically returns the current view of the configuration.
   *
   * @return Current view.
   */
  def view: View = shared {
    this.current.get
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
  def apply(address: Address, backoff: Duration): Proposer = {
    val ip = ByteBuffer.wrap(InetAddress.getByName(address.name).getAddress).getInt
    val id = (ip.toLong << 32) | (address.port & 0xffffffffL)
    new Proposer(id, Cluster.empty, Cluster.empty, backoff)
  }

}
