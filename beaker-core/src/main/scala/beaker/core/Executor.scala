package beaker.core

import beaker.common.concurrent.Task
import beaker.common.relation.Relation

import java.io.Closeable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * A command scheduler. Executors guarantee linearized execution of related commands; for any
 * commands A ~ B, A and B will never be executed simultaneously.
 *
 * @see https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf
 * @param relation Command relation.
 */
class Executor[T](relation: Relation[T]) extends Closeable {

  private[this] var epoch    : Long                         = 0L
  private[this] val horizon  : mutable.Map[Long, Condition] = mutable.Map.empty
  private[this] val schedule : mutable.Map[T, Long]         = mutable.Map.empty
  private[this] val lock     : ReentrantLock                = new ReentrantLock
  private[this] var barrier  : CountDownLatch               = new CountDownLatch(0)

  // Indefinitely and asynchronously perform all scheduled tasks in the current epoch, block
  // until they all complete, and increment the epoch.
  private[this] val clock = Task.indefinitely {
    this.lock.lock()
    try {
      this.horizon.get(this.epoch + 1).filter(this.lock.getWaitQueueLength(_) > 0) foreach { tick =>
        this.barrier = new CountDownLatch(this.lock.getWaitQueueLength(tick))
        this.epoch += 1
        tick.signalAll()
      }
    } finally {
      this.lock.unlock()
      this.barrier.await()
    }
  }

  override def close(): Unit = {
    this.clock.cancel()
    this.barrier.await()
  }

  /**
   * Asynchronously executes the command and returns the result. Blocks until the command has been
   * scheduled, so that if a thread submits A before B and A ~ B, then A will be executed
   * before B. Commands are greedily scheduled; let S(T) denote the epoch in which a task T has been
   * scheduled. For any task A, S(A) = max(S(B)) + 1 for all B such that A ~ B.
   *
   * @param arg Command argument.
   * @param command Command to execute.
   * @return Future containing result of command execution.
   */
  def submit[U](arg: T)(command: T => Try[U]): Future[U] = {
    val scheduled = new CountDownLatch(1)
    val promise = Promise[U]()

    val execute = new Thread(() => {
      this.lock.lock()
      try {
        val deps = this.schedule.filterKeys(relation.related(_, arg))
        val date = if (deps.isEmpty) this.epoch + 1 else deps.values.max + 1
        this.schedule += arg -> date
        scheduled.countDown()
        this.horizon.getOrElseUpdate(date, this.lock.newCondition()).await()
        this.schedule -= arg
      } finally {
        this.lock.unlock()
        promise.complete(command(arg))
        this.barrier.countDown()
      }
    })

    execute.start()
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