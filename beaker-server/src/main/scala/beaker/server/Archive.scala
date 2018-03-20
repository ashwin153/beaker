package beaker.server

import beaker.common.concurrent.Executor
import beaker.common.util._
import beaker.server.Archive._
import beaker.server.protobuf._

import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * An asynchronous database that linearizes related commands.
 *
 * @param underlying Underlying database.
 * @param executor Transaction executor.
 */
case class Archive(
  underlying: Database,
  executor: Executor[Command] = Executor()
) {

  /**
   * Closes the database and the executor.
   */
  def close(): Unit = {
    this.underlying.close()
    this.executor.close()
  }

  /**
   * Asynchronously retrieves the specified keys. Reads see the effect of all completed writes.
   *
   * @param keys Keys to retrieved.
   * @param ec Execution context.
   * @return Retrieved revisions.
   */
  def read(keys: Set[Key])(implicit ec: ExecutionContext): Future[Map[Key, Revision]] =
    executor.submit(Read(keys))(_ => this.underlying.read(keys))

  /**
   * Asynchronously applies the specified changes.
   *
   * @param changes Changes to apply.
   * @param ec Execution context.
   * @return Whether or not the changes were applied.
   */
  def write(changes: Map[Key, Revision])(implicit ec: ExecutionContext): Future[Unit] =
    commit(Transaction(Map.empty, changes))

  /**
   * Asynchronously commits the transaction. Transactions are atomic, consistent and isolated.
   *
   * @param transaction Transaction to commit.
   * @param ec Execution context.
   * @return Whether or not the transaction was committed.
   */
  def commit(transaction: Transaction)(implicit ec: ExecutionContext): Future[Unit] =
    executor.submit(Commit(transaction))(_ => this.underlying.commit(transaction))

  /**
   * Asynchronously scans the database. Scans do not prevent concurrent writes; therefore, they are
   * not guaranteed to return a consistent snapshot.
   *
   * @param stream Output revisions.
   * @return Database iterator.
   */
  def scan(stream: StreamObserver[Revisions]): StreamObserver[Range] = new StreamObserver[Range] {
    override def onError(throwable: Throwable): Unit =
      stream.onError(throwable)

    override def onCompleted(): Unit =
      stream.onCompleted()

    override def onNext(range: Range): Unit =
      executor.submit(Scan)(_ => underlying.scan(range.after, range.limit) match {
        case Success(r) => Success(stream.onNext(Revisions(r)))
        case Failure(e) => Success(stream.onError(e))
      })
  }

}

object Archive {

  /**
   * An archive operation.
   */
  sealed trait Command
  case class Read(keys: Set[Key]) extends Command
  case class Commit(transaction: Transaction) extends Command
  case object Scan extends Command

  // Scans and commits, conflicting commits, and conflicting reads and commits are related.
  implicit val commandRelation: Relation[Command] = (x, y) => (x, y) match {
    case (Commit(_), Scan) | (Scan, Commit(_)) => true
    case (Commit(t), Commit(u)) => t ~ u
    case (Commit(t), Read(k)) => t.changes.keys.exists(k.contains)
    case (Read(k), Commit(t)) => t.changes.keys.exists(k.contains)
    case _ => false
  }


}