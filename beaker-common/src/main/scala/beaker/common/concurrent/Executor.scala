package beaker.common.concurrent

import beaker.common.util.Relation

import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}
import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * A command scheduler. Executors guarantee linearized execution of related commands, but concurrent
 * execution of unrelated commands. Commands are greedily scheduled; they will be scheduled as
 * early as possible.
 *
 * @see https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf
 * @param relation Command relation.
 */
class Executor[T](relation: Relation[T]) {

  private[this] var epoch: Long = 0L
  private[this] val horizon: mutable.Map[Long, Condition] = mutable.Map.empty
  private[this] val schedule: mutable.Map[T, Long] = mutable.Map.empty
  private[this] val lock: ReentrantLock = new ReentrantLock
  private[this] var barrier: CountDownLatch = new CountDownLatch(0)
  private[this] val nonEmpty: Condition = this.lock.newCondition()
  private[this] val underlying: ExecutorService = Executors.newCachedThreadPool()

  private[this] val clock = Task.indefinitely {
    this.lock.lock()
    try {
      this.horizon.get(this.epoch + 1) match {
        case Some(waiting) if this.lock.getWaitQueueLength(waiting) > 0 =>
          // Wait for all transactions in the current epoch to complete.
          this.barrier = new CountDownLatch(this.lock.getWaitQueueLength(waiting))
          this.epoch += 1
          waiting.signalAll()
        case _ =>
          // Wait until a transaction is scheduled.
          this.nonEmpty.await()
      }
    } finally {
      this.lock.unlock()
      this.barrier.await()
    }
  }

  def close(): Unit = {
    this.clock.cancel()
    this.barrier.await()
  }

  /**
   * Asynchronously executes the command and returns the result. Blocks until the command has been
   * scheduled, so that if a thread submits A before B and A ~ B, then A will be executed
   * before B.
   *
   * @param arg Command argument.
   * @param command Command to execute.
   * @return Future containing result of command execution.
   */
  def submit[U](arg: T)(command: T => Try[U]): Future[U] = {
    val scheduled = new CountDownLatch(1)
    val promise = Promise[U]()

    this.underlying.submit(() => {
      this.lock.lock()
      try {
        // Schedule the transaction in the first available epoch.
        val deps = this.schedule.filterKeys(relation.related(_, arg))
        val date = if (deps.isEmpty) this.epoch + 1 else deps.values.max + 1
        this.schedule += arg -> date
        scheduled.countDown()

        // Wait for its epoch.
        val wait = this.horizon.getOrElseUpdate(date, this.lock.newCondition())
        this.nonEmpty.signalAll()
        wait.await()

        // Remove the transaction from the schedule.
        this.schedule -= arg
      } finally {
        this.lock.unlock()
        promise.complete(command(arg))
        this.barrier.countDown()
      }
    })

    scheduled.await()
    promise.future
  }

}

object Executor {

  /**
   * Constructs an executor from the implicit relation.
   *
   * @param relation Command relation.
   * @return Executor on relation.
   */
  def apply[T]()(implicit relation: Relation[T]): Executor[T] = new Executor[T](relation)

}