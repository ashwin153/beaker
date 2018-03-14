package beaker.core

import beaker.cluster
import beaker.cluster.{Address, Cluster}
import beaker.common.concurrent._
import beaker.common.relation._
import beaker.core.storage.Local
import beaker.core.protobuf._

import io.grpc.ServerBuilder

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.language.postfixOps
import scala.math.Ordering.Implicits._
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
 * A distributed, transactional key-value store.
 *
 * @param id Unique identifier.
 * @param database Underlying database.
 * @param executor Transaction executor.
 * @param cluster Beaker cluster.
 * @param backoff Consensus backoff duration.
 */
case class Beaker(
  id: Int,
  database: Database,
  executor: Executor[Transaction],
  cluster: Cluster[Internal.Client],
  backoff: Duration = 1 second,
  quorum: Int
) extends BeakerGrpc.Beaker with Closeable {

  var round    : AtomicInteger                  = new AtomicInteger(1)
  val proposed : mutable.Map[Transaction, Task] = mutable.Map.empty
  val promised : mutable.Set[Proposal]          = mutable.Set.empty
  val accepted : mutable.Set[Proposal]          = mutable.Set.empty
  val learned  : mutable.Map[Proposal, Int]     = mutable.Map.empty

  /**
   * Coordinate consensus on a proposal. Uses a variation of Generalized Paxos that has several
   * desirable properties. First, beakers may simultaneously commit non-conflicting transactions.
   * Second, beakers automatically repair replicas that have stale revisions. Third, beakers may
   * safely commit transactions as long as they are connected to at least a majority of their
   * non-faulty peers.
   *
   * @see https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-33.pdf
   * @see https://www.datastax.com/dev/blog/lightweight-transactions-in-cassandra-2-0
   * @see https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf
   *
   * @param proposal Proposed proposal.
   * @return Failure on error, loops indefinitely otherwise.
   */
  def consensus(proposal: Proposal): Try[Unit] = {
    // Prepare the proposal on a quorum of beakers.
    this.cluster.quorum(_.prepare(proposal)) flatMap { promises =>
      val maximum = promises.map(_.getBallot).max

      if (maximum > proposal.getBallot) {
        // If a promise has been made to a newer proposal, then retry with a higher ballot.
        Thread.sleep(backoff.toMillis)
        val newest = this.round.updateAndGet(i => (i max maximum.round) + 1)
        val ballot = Ballot(newest, this.id)
        consensus(proposal.withBallot(ballot))
      } else {
        // Otherwise, merge the returned promises into a single proposal.
        val promise = promises.reduce[Proposal](merge).copy(ballot = proposal.ballot)

        if (!matches(proposal, proposal)) {
          // If the promise does not match the proposal, then retry with the promise.
          Thread.sleep(backoff.toMillis)
          val newest = this.round.getAndIncrement()
          val ballot = Ballot(newest, this.id)
          consensus(promise.withBallot(ballot))
        } else {
          // Otherwise, get the keys that read by the promise from a quorum of beakers.
          val depends = promise.applies.flatMap(_.depends.keySet)
          val changes = promise.applies.flatMap(_.changes.keySet)

          this.cluster.quorum(_.get(depends.toSet)) filter { replicas =>
            // Determine the latest and the oldest version of each key.
            val latest   = replicas.reduce(merge(_, _))
            val oldest   = replicas.reduce(merge(_, _)(revisionOrdering.reverse))
            val snapshot = Local.Database(latest)

            // Discard all transactions in the promise that cannot be committed and repair all keys
            // that are read - but not written - by the transaction with different revisions.
            val applies = promise.applies.filter(snapshot.commit(_).isSuccess)
            val stale   = (latest -- changes) filter { case (k, r) => oldest(k) < r }
            val repairs = merge(proposal.getRepairs, Transaction(Map.empty, stale))

            // Filter accepted proposals that do not contain any transactions or repairs.
            val accept = promise.copy(applies = applies, repairs = Some(repairs))
            accept.applies.nonEmpty || accept.getRepairs.changes.nonEmpty
          } flatMap { _ =>
            // Otherwise, send the promise to a quorum of beakers and retry automatically.
            this.cluster.quorum(_.accept(promise))
            Thread.sleep(backoff.toMillis)
            consensus(promise)
          }
        }
      }
    }
  }

  override def get(keys: Keys): Future[Revisions] = {
    // Performs a read-only transaction on the underlying database.
    val depends  = keys.contents.map(_ -> 0L).toMap
    val readOnly = Transaction(depends, Map.empty)
    val latest   = this.executor.submit(readOnly)(t => this.database.read(t.depends.keySet))
    latest recover { case _ => Map.empty[Key, Revision] } map Revisions.apply
  }

  override def propose(transaction: Transaction): Future[Result] = {
    val ballot = Ballot(this.round.getAndIncrement(), this.id)
    val proposal = Proposal(Some(ballot), Seq(transaction))

    // Asynchronously attempt to reach consensus on the proposal.
    val daemon = Task(consensus(proposal))
    this.proposed += transaction -> daemon
    daemon.future map { _ => Result(true) } recover { case _ => Result(false) }
  }

  override def prepare(proposal: Proposal): Future[Proposal] = synchronized {
    this.promised.find(_ |> proposal) match {
      case Some(r) =>
        // If a promise has been made to a newer proposal, its ballot is returned.
        Future(Proposal(r.ballot))
      case None =>
        // Otherwise, the beaker promises not to accept any proposal that conflicts with the
        // proposal it returns that has a lower ballot than the proposal it receives. If a beaker
        // has already accepted older proposals, it merges them together and returns the result.
        // Otherwise, it returns the proposal with the zero ballot.
        val accept = this.accepted.filter(_ <| proposal)
        val promise = accept.reduceOption[Proposal](merge).getOrElse(proposal.withBallot(Ballot()))
        this.promised --= this.promised.filter(_ <| proposal)
        this.promised += promise.copy(ballot = proposal.ballot)
        Future(promise)
    }
  }

  override def accept(proposal: Proposal): Future[Result] = synchronized {
    if (!this.promised.exists(p => p.ballot < proposal.ballot && (p |> proposal))) {
      // If the beaker has not promised not to accept the proposal, then it accepts the proposal and
      // broadcasts a vote for it.
      this.accepted --= this.accepted.filter(_ <| proposal)
      this.accepted += proposal
      this.cluster.broadcast(_.learn(proposal))
      Future(Result(true))
    } else {
      // Otherwise, it rejects the proposal.
      Future(Result(false))
    }
  }

  override def learn(proposal: Proposal): Future[Void] = synchronized {
    // Vote for the proposal and discard older learned proposals.
    val votes = this.learned.getOrElse(proposal, 0)
    this.learned --= this.learned.keys.filter(_ <| proposal)
    this.learned(proposal) = votes + 1

    if (this.learned(proposal) == quorum) {
      // Commit its transactions and repairs and discard older accepted proposals.
      val transactions = proposal.applies ++ proposal.repairs
      transactions.foreach(this.executor.submit(_)(this.database.commit))
      this.accepted.retain(p => !(p <| proposal))

      // Complete consensus on all conflicting proposed transactions completes.
      this.proposed --= this.proposed collect {
        case (t, u) if transactions.contains(t)   => u.finish(); t
        case (t, u) if transactions.exists(_ ~ t) => u.cancel(); t
      }
    }

    Future(Void())
  }

  override def close(): Unit = {
    this.database.close()
    this.executor.close()
  }

}

object Beaker {

  /**
   * A Beaker server.
   *
   * @param port Port number.
   * @param beaker Beaker service.
   */
  case class Server(
    port: Int,
    beaker: Beaker
  ) extends cluster.Server {

    private val underlying = ServerBuilder
      .forPort(this.port)
      .addService(BeakerGrpc.bindService(beaker, ExecutionContext.global))
      .build()

    override val address: Address =
      Address.local(this.port)

    override def serve(): Unit = {
      this.beaker.cluster.join(this.address)
      this.underlying.start()
    }

    override def close(): Unit = {
      this.beaker.cluster.leave(this.address)
      this.underlying.shutdown()
    }

  }

}