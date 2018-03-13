package caustic.beaker.common

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * An asynchronous, interruptible computation. Tasks are futures that respond to interrupts.
 * Tasks are particularly useful for representing computations that run indefinitely until some
 * external condition is satisfied.
 */
class Task(body: => Try[Unit]) {

  private val promise: Promise[Unit] = Promise()
  private val thread: Thread = new Thread(() => {
    try {
      promise.complete(body)
    } catch {
      case _: InterruptedException =>
    }
  })

  // Begin executing the body immediately.
  this.thread.start()

  /**
   * Returns a future that completes when the task terminates.
   *
   * @return Computation handle.
   */
  def future: Future[Unit] = {
    this.promise.future
  }

  /**
   * Completes the task successfully.
   */
  def finish(): Unit = {
    this.thread.interrupt()
    this.promise.success(())
  }


  /**
   * Completes the task unsuccessfully.
   */
  def cancel(): Unit = {
    this.thread.interrupt()
    this.promise.failure(Task.Cancelled)
  }

}

object Task {

  /**
   * A failure that indicates a task was cancelled.
   */
  case object Cancelled extends Exception

  /**
   * Constructs a task that performs the body.
   *
   * @param body Computation.
   * @return Asynchronous task.
   */
  def apply(body: => Try[Unit]): Task = new Task(body)

  /**
   * Constructs a task that indefinitely performs the body.
   *
   * @param body Computation.
   * @return Indefinite task.
   */
  def indefinitely[T](body: => T): Task =
    Task { Try { while (true) { body; Thread.`yield`() } } }

  /**
   * Constructs a task that periodically performs the body.
   *
   * @param delay Period.
   * @param body Computation.
   * @return Periodic task.
   */
  def periodically[T](delay: Duration)(body: => T): Task =
    Task { Try { body; Thread.sleep(delay.toMillis) } }

}