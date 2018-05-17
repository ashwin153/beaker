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
    !xr.disjoint(yw) || !yr.disjoint(xw) || !xw.disjoint(yw)
  }

  // Proposals are partially ordered by their view and by ballot when their transactions conflict.
  implicit val proposalOrder: PartialOrder[Proposal] = (x, y) => {
    if (x.view == y.view)
      x.commits.find(t => y.commits.exists(_ ~ t)).map(_ => x.ballot < y.ballot)
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
     * Returns whether or not the proposal conflicts with the specified sequence of transactions.
     *
     * @param y Transactions.
     * @return Whether or not they conflict.
     */
    def conflicts(y: Seq[Transaction]): Boolean =
      x.commits.exists(t => y.contains(t) || y.exists(_ ~ t))

    /**
     * Returns whether or not the proposal commit the same transactions in the same configuration.
     *
     * @param y A proposal.
     * @return Whether or not they are equivalent.
     */
    def equivalent(y: Proposal): Boolean =
      x.commits == y.commits

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
      val commits = latest.commits ++ oldest.commits.filterNot(t => latest.commits.exists(_ ~ t))
      val repairs = latest.repairs maximum oldest.repairs
      latest.copy(ballot = latest.ballot max oldest.ballot, commits = commits, repairs = repairs)
    }

  }

}
