package beaker.common.concurrent

import beaker.common.concurrent.Executor.Command
import beaker.common.util._

import java.util.concurrent._
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * A command scheduler. Executors guarantee linearized execution of related commands, but concurrent
 * execution of unrelated commands. Commands are greedily scheduled; they will be scheduled as
 * early as possible.
 *
 * @see https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf
 * @param schedule Request queue.
 * @param worker Worker pool.
 * @param relation Command relation.
 */
class Executor[T](
  schedule: LinkedBlockingQueue[Command[T]],
  worker: ExecutorService
)(
  implicit relation: Relation[T]
) {

  // Concurrently performs as many unrelated commands as possible.
  private[this] val clock: Task = Task.indefinitely {
    val group = mutable.Buffer(this.schedule.take())
    while (this.schedule.peek() != null && !group.exists(cmd => cmd.arg ~ this.schedule.peek().arg))
      group += this.schedule.take()
    group.map(cmd => this.worker.submit(cmd.run)).foreach(_.get())
  }

  /**
   * Shutdowns the clock and the thread pool.
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
   * @param arg Argument.
   * @param f Function.
   * @return Future containing result of command execution.
   */
  def submit[U](arg: T)(f: T => Try[U]): Future[U] = {
    val promise = Promise[U]()
    this.schedule.offer(Command(arg, () => promise.complete(f(arg))))
    promise.future
  }

}

object Executor {

  /**
   * An executable command.
   *
   * @param arg Argument.
   * @param run Function.
   */
  case class Command[T](arg: T, run: Runnable)

  /**
   * Constructs an executor over a cached thread pool.
   *
   * @param relation Command relation.
   * @return Executor.
   */
  def apply[T]()(implicit relation: Relation[T]): Executor[T] =
    Executor(Executors.newCachedThreadPool())

  /**
   * Constructs an executor over the specified worker pool.
   *
   * @param worker Worker pool.
   * @param relation Command relation.
   * @return Executor.
   */
  def apply[T](worker: ExecutorService)(implicit relation: Relation[T]): Executor[T] =
    new Executor(new LinkedBlockingQueue(), worker)

}