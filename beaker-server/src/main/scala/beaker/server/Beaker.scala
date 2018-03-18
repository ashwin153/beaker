package beaker.server

import beaker.common.util._
import beaker.common.concurrent._
import beaker.server.storage.Local
import beaker.server.protobuf._
import beaker.server.service.Router

import io.grpc.stub.StreamObserver

import java.io.Closeable
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.language.postfixOps
import scala.math.Ordering.Implicits._
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * A distributed, transactional key-value store.
 *
 * @param address Unique identifier.
 * @param database Underlying database.
 * @param initial Initial configuration.
 * @param executor Transaction executor.
 * @param backoff Consensus backoff duration.
 */
case class Beaker(
  address: Address,
  database: Database,
  initial: Configuration,
  executor: Executor[Transaction] = Executor(),
  backoff: Duration = 1 second
) extends BeakerGrpc.Beaker with Closeable {

  // Construct a globally unique identifier for the beaker from its address.
  val ip = ByteBuffer.wrap(InetAddress.getByName(address.name).getAddress).getInt
  val id = (this.ip.toLong << 32) | (this.address.port & 0xffffffffL)

  var round         : AtomicInteger                  = new AtomicInteger(1)
  var router        : Router                         = Router(initial)
  val proposed      : mutable.Map[Proposal, Task]    = mutable.Map.empty
  val promised      : mutable.Set[Proposal]          = mutable.Set.empty
  val accepted      : mutable.Set[Proposal]          = mutable.Set.empty
  val learned       : mutable.Map[Proposal, Int]     = mutable.Map.empty

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
    this.router.quorum(_.prepare(proposal)) flatMap { promises =>
      val maxBallot = promises.maxBy(_.getBallot).getBallot
      val maxConfig = promises.maxBy(_.getConfiguration).getConfiguration

      if (maxBallot > proposal.getBallot) {
        // If a promise has been made to a newer proposal, then retry with a higher ballot.
        Thread.sleep(backoff.toMillis)
        val newest = this.round.updateAndGet(i => (i max maxBallot.round) + 1)
        val ballot = Ballot(newest, this.id)
        consensus(proposal.copy(ballot = Some(ballot)))
      } else if (maxConfig > this.router.configuration) {
        // If a newer configuration exists, then retry with the new configuration.
        Thread.sleep(backoff.toMillis)
        val newest = this.round.getAndIncrement()
        val ballot = Ballot(newest, this.id)
        consensus(proposal.copy(ballot = Some(ballot), configuration = Some(maxConfig)))
      } else {
        // Otherwise, merge the returned promises into a single proposal.
        val promise = promises.reduce(_ merge _).copy(ballot = proposal.ballot)

        if (promise matches proposal) {
          // If the proposal matches the promise, get all keys read by the promise from a quorum.
          val depends = promise.applies.flatMap(_.depends.keySet)
          val changes = promise.applies.flatMap(_.changes.keySet)

          this.router.quorum(_.get(Keys(depends))) map { replicas =>
            // Determine the latest and the oldest version of each key.
            val latest = replicas.map(_.entries).reduce(_ maximum _)
            val oldest = replicas.map(_.entries).reduce(_ minimum _)
            val snapshot = Local.Database(latest)

            // Discard all transactions in the promise that cannot be committed and repair all keys
            // that are read - but not written - by the transaction with different revisions.
            val applies = promise.applies.filter(snapshot.commit(_).isSuccess)
            val stale = (latest -- changes) filter { case (k, r) => oldest(k) < r }
            val repairs = proposal.getRepairs merge Transaction(Map.empty, stale)
            promise.copy(applies = applies, repairs = Some(repairs))
          } filter { updated =>
            // Filter proposal that contain transactions or repairs.
            updated.applies.nonEmpty || updated.getRepairs.changes.nonEmpty
          } flatMap { updated =>
            // Send the updated promise to a quorum of beakers and retry automatically.
            this.router.quorum(_.accept(updated))
            Thread.sleep(backoff.toMillis)
            consensus(updated)
          }
        } else {
          // Otherwise, retry with the promise.
          Thread.sleep(backoff.toMillis)
          val newest = this.round.getAndIncrement()
          val ballot = Ballot(newest, this.id)
          consensus(promise.copy(ballot = Some(ballot)))
        }
      }
    }
  }

  /**
   *
   * @param changes
   * @return
   */
  def repair(changes: Map[Key, Revision]): Future[Unit] = {
    //
    executor.submit(Transaction(Map.empty, changes))(database.commit)
  }

  override def get(keys: Keys): Future[Revisions] = {
    // Performs a read-only transaction on the underlying database.
    val readOnly = Transaction(keys.names.map(_ -> 0L).toMap, Map.empty)
    val latest   = this.executor.submit(readOnly)(t => this.database.read(t.depends.keySet))
    latest recover { case _ => Map.empty[Key, Revision] } map Revisions.apply
  }

  override def scan(revisions: StreamObserver[Revisions]) = new StreamObserver[Range] {
    override def onError(throwable: Throwable): Unit =
      revisions.onError(throwable)

    override def onCompleted(): Unit =
      revisions.onCompleted()

    override def onNext(range: Range): Unit =
      database.scan(Option(range.after), range.limit) match {
        case Success(r) => revisions.onNext(Revisions(r))
        case Failure(e) => revisions.onError(e)
      }
  }

  override def view(void: Void): Future[Cluster] = {
    //
    Future(this.router.configuration.getCluster)
  }

  override def reconfigure(cluster: Cluster): Future[Result] = {
    val ballot = Ballot(this.round.getAndIncrement(), this.id)
    val config = this.router.configuration.copy(ballot = Some(ballot), cluster = Some(cluster))
    val proposal = Proposal(Some(ballot), Seq.empty, None, Some(config))

    // Asynchronously attempt to reach consensus on the proposal.
    val daemon = Task(consensus(proposal))
    this.proposed += proposal -> daemon
    daemon.future map { _ => Result(true) } recover { case _ => Result(false) }
  }

  override def propose(transaction: Transaction): Future[Result] = {
    val ballot = Ballot(this.round.getAndIncrement(), this.id)
    val proposal = Proposal(Some(ballot), Seq(transaction), None, Some(this.router.configuration))

    // Asynchronously attempt to reach consensus on the proposal.
    val daemon = Task(consensus(proposal))
    this.proposed += proposal -> daemon
    daemon.future map { _ => Result(true) } recover { case _ => Result(false) }
  }

  override def prepare(proposal: Proposal): Future[Proposal] = synchronized {
    if (proposal.getConfiguration < this.router.configuration) {
      // If a proposal has an outdated configuration, then return the current configuration.
      Future(Proposal(proposal.ballot, Seq.empty, None, Some(this.router.configuration)))
    } else {
      // Otherwise, return a promise.
      this.promised.find(_ |> proposal) match {
        case Some(r) =>
          // If a promise has been made to a newer proposal, its ballot is returned.
          Future(Proposal(r.ballot, Seq.empty, None, r.configuration))
        case None =>
          // Otherwise, the beaker promises not to accept any proposal that conflicts with the
          // proposal it returns that has a lower ballot than the proposal it receives. If a beaker
          // has already accepted older proposals, it merges them together and returns the result.
          // Otherwise, it returns the proposal with the zero ballot.
          val accept = this.accepted.filter(_ <| proposal)
          val promise = accept.reduceOption(_ merge _).getOrElse(proposal.withBallot(Ballot()))
          this.promised --= this.promised.filter(_ <| proposal)
          this.promised += promise.copy(ballot = proposal.ballot)
          Future(promise)
      }
    }
  }

  override def accept(proposal: Proposal): Future[Result] = synchronized {
    if (!this.promised.exists(p => p.getBallot < proposal.getBallot && (p |> proposal))) {
      // If the beaker has not promised not to accept the proposal, then it votes for it.
      this.accepted --= this.accepted.filter(_ <| proposal)
      this.accepted += proposal
      this.router.broadcast(_.learn(proposal))
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

    if (this.learned(proposal) == proposal.getConfiguration.getCluster.quorum) {
      // Commit its transactions and repairs and discard older accepted proposals.
      val transactions = proposal.applies ++ proposal.repairs
      transactions.foreach(this.executor.submit(_)(this.database.commit))
      this.accepted.retain(p => p.getBallot != proposal.getBallot && !(p <| proposal))

      // Update the current configuration with the proposal's configuration.
      this.router.reconfigure(proposal.getConfiguration)

      // Complete consensus on all conflicting proposals completes.
      this.proposed --= this.proposed.filterKeys(_ <| proposal) collect {
        case (p, t) if p == proposal => t.finish(); p
        case (p, t) => t.cancel(); p
      }
    }

    Future(Void())
  }

  override def close(): Unit = {
    this.database.close()
    this.executor.close()
  }

}