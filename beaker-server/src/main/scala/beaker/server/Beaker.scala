package beaker.server

import beaker.common.util._
import beaker.common.concurrent._
import beaker.server.protobuf._

import com.typesafe.scalalogging.LazyLogging
import io.grpc.stub.StreamObserver

import scala.collection.mutable
import scala.Console._
import scala.language.postfixOps
import scala.math.Ordering.Implicits._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * A distributed, transactional key-value store.
 *
 * @param archive Underlying archive.
 * @param proposer Consensus coordinator.
 */
case class Beaker(
  archive: Archive,
  proposer: Proposer
) extends BeakerGrpc.Beaker with LazyLogging {

  private[this] val configuring: mutable.Map[View, Task] = mutable.Map.empty
  private[this] val proposing: mutable.Map[Transaction, Task] = mutable.Map.empty
  private[this] val prepared: mutable.Set[Proposal] = mutable.Set.empty
  private[this] val accepted: mutable.Set[Proposal] = mutable.Set.empty
  private[this] val learned: mutable.Map[Proposal, Int] = mutable.Map.empty

  /**
   * Closes the archive and the proposer.
   */
  def close(): Unit = {
    this.archive.close()
    this.proposer.close()
  }

  override def get(keys: Keys): Future[Revisions] = {
    this.archive.read(keys.names.toSet) map Revisions.apply
  }

  override def scan(revisions: StreamObserver[Revisions]): StreamObserver[Range] = {
    this.archive.scan(revisions)
  }

  override def network(void: Void): Future[View] = {
    Future(this.proposer.view)
  }

  override def reconfigure(configuration: Configuration): Future[Result] = synchronized {
    if (this.configuring.nonEmpty) {
      // If a view is already being configured, then return failure.
      Future(Result(false))
    } else {
      // Otherwise, asynchronously propose the new configuration.
      val view = View(this.proposer.next(), configuration)
      val proposal = Proposal(view.ballot, Seq.empty, Map.empty, view)
      val task = Task(this.proposer.consensus(proposal))
      this.configuring += view -> task
      task.future map { _ => Result(true) } recover { case _ => Result(false) }
    }
  }

  override def propose(transaction: Transaction): Future[Result] = synchronized {
    if (this.proposing.keys.exists(_ ~ transaction)) {
      // If the transaction conflicts with a proposed transaction, then return failure.
      Future(Result(false))
    } else {
      // Otherwise, asynchronously propose the transaction.
      val proposal = Proposal(this.proposer.next(), Seq(transaction), Map.empty, this.proposer.view)
      val task = Task(this.proposer.consensus(proposal))
      this.proposing += transaction -> task
      task.future map { _ => Result(true) } recover { case _ => Result(false) }
    }
  }

  override def prepare(proposal: Proposal): Future[Proposal] = synchronized {
    this.prepared.find(proposal <| _) match {
      case Some(r) =>
        // If a promise has been made to a newer proposal, its ballot is returned.
        this.logger.debug(s"${ RED }Rejected${ RESET }  ${ proposal.commits.hashCode() }")
        Future(Proposal(ballot = r.ballot, view = this.proposer.view max proposal.view))
      case None =>
        // Otherwise, any older accepted proposals are merged together into a promise or the
        // proposal is promised with the zero ballot if no older proposals have been accepted.
        val promise = this.accepted.filter(_ <| proposal)
          .reduceOption(_ merge _)
          .getOrElse(proposal.withBallot(Ballot.defaultInstance))
          .withView(this.proposer.view max proposal.view)

        // Promises not to accept any proposal that is older than the promised proposal.
        this.prepared --= this.prepared.filter(_ <| proposal)
        this.prepared += promise.withBallot(proposal.ballot)
        this.logger.debug(s"${ YELLOW }Prepared${ RESET }  ${ promise.commits.hashCode() }")
        Future(promise)
    }
  }

  override def accept(proposal: Proposal): Future[Result] = synchronized {
    if (!this.prepared.exists(_ |> proposal)) {
      // If it has not promised not to accept the proposal, then it votes for the proposal.
      this.accepted --= this.accepted.filter(_ <| proposal)
      this.accepted += proposal
      this.proposer.learners.broadcastAsync(_.learn(proposal))
      this.logger.debug(s"${ BLUE }Accepted${ RESET }  ${ proposal.commits.hashCode() }")
      Future(Result(true))
    } else {
      // Otherwise, it rejects the proposal.
      this.logger.debug(s"${ RED }Rejected${ RESET }  ${ proposal.commits.hashCode()}")
      Future(Result(false))
    }
  }

  override def learn(proposal: Proposal): Future[Void] = synchronized {
    // Vote for the proposal and discard older learned proposals.
    this.logger.debug(s"${ GREEN }Learning${ RESET }  ${ proposal.commits.hashCode() }")
    this.learned.removeKeys(_ <| proposal)
    this.learned(proposal) = this.learned.getOrElse(proposal, 0) + 1

    if (this.learned(proposal) == this.proposer.acceptors.size / 2 + 1) {
      // If the proposal receives a majority of votes, then commit it.
      val commits = proposal.commits :+ Transaction(Map.empty, proposal.repairs)
      commits.foreach(this.archive.commit)
      this.proposer.reconfigure(proposal.view)
      this.prepared --= this.prepared.filter(_ conflicts commits)
      this.accepted --= this.accepted.filter(_ conflicts commits)

      this.proposing.removeKeys(commits.contains).values.foreach(_.finish())
      this.proposing.removeKeys(t => commits.exists(_ ~ t)).values.foreach(_.cancel())
      this.configuring.removeKeys(_ == proposal.view).values.foreach(_.finish())
      this.configuring.removeKeys(_ < proposal.view).values.foreach(_.cancel())
      this.logger.debug(s"${ GREEN }Learned${ RESET }   ${ proposal.commits.hashCode() }")
    }

    Future(Void())
  }

}
