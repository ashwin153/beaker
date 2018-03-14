package beaker

import beaker.common.relation._
import beaker.core.protobuf._

import scala.math.Ordering.Implicits._

package object core {

  type Key = String

  // Ballots are totally ordered by their (round, id).
  implicit val ballotOrdering: Ordering[Ballot] = (x, y) => {
    if (x.round == y.round) x.id compare y.id else x.round compare y.round
  }

  // Revisions are uniquely identified and totally ordered by their version.
  implicit val revisionOrdering: Ordering[Revision] = (x, y) => {
    x.version compare y.version
  }

  // Proposals are partially ordered by their ballot whenever their transactions conflict.
  implicit val proposalOrder: Order[Proposal] = (x, y) => {
    x.applies.find(t => y.applies.exists(_ ~ t)).map(_ => x.getBallot <= y.getBallot)
  }

  // Transactions conflict if either reads or writes a key that the other writes.
  implicit val transactionRelation: Relation[Transaction] = (x, y) => {
    val (xr, xw) = (x.depends.keySet, x.changes.keySet)
    val (yr, yw) = (y.depends.keySet, y.changes.keySet)
    xr.intersect(yw).nonEmpty || yr.intersect(xw).nonEmpty || xw.intersect(yw).nonEmpty
  }

  /**
   * Returns whether the proposals commit the same transactions.
   *
   * @param x A proposal.
   * @param y Another proposal.
   * @return Whether the proposals match.
   */
  def matches(x: Proposal, y: Proposal): Boolean =
    x.applies == y.applies

  /**
   * Merges the older proposal into the newer proposal by discarding all transactions in the older
   * proposal that conflict with transactions in the newer proposal are discarded and merging their
   * repairs.
   *
   * @param x A proposal.
   * @param y Another proposal.
   * @return Union of proposals.
   */
  def merge(x: Proposal, y: Proposal): Proposal = {
    val (latest, oldest) = if (x <| y) (y, x) else (x, y)
    val applies = latest.applies ++ oldest.applies.filterNot(t => latest.applies.exists(_ ~ t))
    val repairs = merge(latest.getRepairs, oldest.getRepairs)
    latest.copy(applies = applies, repairs = Some(repairs))
  }

  /**
   * Merges the transactions together by merging their dependencies and changes.
   *
   * @param x A transaction.
   * @param y Another transaction.
   * @return Union of transactions.
   */
  def merge(x: Transaction, y: Transaction): Transaction = {
    Transaction(merge(x.depends, y.depends), merge(x.changes, y.changes))
  }

  /**
   * Merges two maps together by selecting the larger value in the case of duplicate keys.
   *
   * @param x A map.
   * @param y Another map.
   * @param ordering Value ordering.
   * @return Union of maps.
   */
  def merge[A, B](x: Map[A, B], y: Map[A, B])(implicit ordering: Ordering[B]): Map[A, B] = {
    x ++ y map { case (k, v) => k -> (x.getOrElse(k, v) max v) }
  }

}
