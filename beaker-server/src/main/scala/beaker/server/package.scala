package beaker

import beaker.common.util._
import beaker.server.protobuf._

import scala.math.Ordering.Implicits._

package object server {

  type Key = String

  // Ballots are totally ordered by their (round, id).
  implicit val ballotOrdering: Ordering[Ballot] = (x, y) => {
    if (x.round == y.round) x.id compare y.id else x.round compare y.round
  }

  // Revisions are uniquely identified and totally ordered by their version.
  implicit val revisionOrdering: Ordering[Revision] = (x, y) => {
    x.version compare y.version
  }

  // Views are uniquely identified and totally ordered by their ballot.
  implicit val viewOrdering: Ordering[View] = (x, y) => {
    ballotOrdering.compare(x.ballot, y.ballot)
  }

  // Transactions are related if either reads or writes a key that the other writes.
  implicit val transactionRelation: Relation[Transaction] = (x, y) => {
    val (xr, xw) = (x.depends.keySet, x.changes.keySet)
    val (yr, yw) = (y.depends.keySet, y.changes.keySet)
    xr.intersect(yw).nonEmpty || yr.intersect(xw).nonEmpty || xw.intersect(yw).nonEmpty
  }

  // Proposals are partially ordered by their view and by ballot when their transactions conflict.
  implicit val proposalOrder: Order[Proposal] = (x, y) => {
    if (x.view == y.view)
      x.applies.find(t => y.applies.exists(_ ~ t)).map(_ => x.ballot < y.ballot)
    else
      Some(x.view < y.view)
  }

  implicit class TransactionOps(x: Transaction) {

    /**
     * Merges the transactions together by merging their dependencies and changes.
     *
     * @param y A transaction.
     * @return Union of transactions.
     */
    def merge(y: Transaction): Transaction =
      Transaction(x.depends maximum y.depends, x.changes minimum y.changes)

  }

  implicit class ProposalOps(x: Proposal) {

    /**
     * Returns whether or not the proposal apply the same transactions to the same configuration.
     *
     * @param y A proposal.
     * @return Whether or not they match.
     */
    def matches(y: Proposal): Boolean =
      x.applies == y.applies && x.view == y.view

    /**
     * Merges the older proposal into the newer proposal by discarding all transactions in the older
     * proposal that conflict with transactions in the newer proposal are discarded and merging
     * their repairs.
     *
     * @param y A proposal.
     * @return Union of proposals.
     */
    def merge(y: Proposal): Proposal = {
      val (latest, oldest) = if (x <| y) (y, x) else (x, y)
      val applies = latest.applies ++ oldest.applies.filterNot(t => latest.applies.exists(_ ~ t))
      val repairs = latest.repairs maximum oldest.repairs
      latest.copy(applies = applies, repairs = repairs)
    }

  }

}
