package beaker.common.concurrent

import beaker.common.concurrent.Executor.Command
import beaker.common.util._
import java.util.concurrent._
import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.collection.mutable
import scala.collection.JavaConverters._
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
case class Executor[T](implicit relation: Relation[T]) {

  //
  private[this] val queue: LinkedBlockingQueue[Command[T]] = new LinkedBlockingQueue()
  private[this] val worker: ExecutorService = Executors.newCachedThreadPool()

  //
  private[this] val clock: Task = Task.indefinitely {
    val group = mutable.Buffer(this.queue.take())
    while (this.queue.peek() != null && !group.exists(cmd => cmd.arg ~ this.queue.peek().arg))
      group += this.queue.take()
    group.map(cmd => this.worker.submit(cmd.run)).foreach(_.get())
  }

  /**
   *
   */
  def close(): Unit = {
    this.clock.cancel()
    this.worker.shutdown()
  }

  /**
   * Asynchronously executes the command and returns the result. Blocks until the command has been
   * scheduled, so that if a thread submits A before B and A ~ B, then A will be executed
   * before B.
   *
   * @param arg Command argument.
   * @param f Command to execute.
   * @return Future containing result of command execution.
   */
  def submit[U](arg: T)(f: T => Try[U]): Future[U] = {
    val promise = Promise[U]()
    this.queue.offer(Command(arg, () => promise.complete(f(arg))))
    promise.future
  }

}

object Executor {

  /**
   *
   * @param arg
   * @param run
   * @tparam T
   */
  case class Command[T](arg: T, run: Runnable)

}