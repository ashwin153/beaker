package beaker.common.concurrent

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * An asynchronous, interruptible computation. Tasks are futures that respond to interrupts.
 * Tasks are particularly useful for representing computations that run indefinitely until some
 * external condition is satisfied.
 */
class Task(body: => Try[Unit]) extends Runnable {

  private[this] val promise: Promise[Unit] = Promise()
  private[this] val running: AtomicReference[Thread] = new AtomicReference[Thread]()

  /**
   * Returns a future that completes when the task terminates.
   *
   * @return Computation handle.
   */
  def future: Future[Unit] = this.promise.future

  /**
   * Completes the task unsuccessfully.
   */
  def cancel(): Unit = synchronized {
    if (!this.promise.isCompleted) {
      val thread = this.running.get()
      if (thread != null) thread.interrupt()
      this.promise.failure(Task.Cancelled)
    }
  }

  /**
   * Completes the task successfully.
   */
  def finish(): Unit = synchronized {
    if (!this.promise.isCompleted) {
      val thread = this.running.get()
      if (thread != null) thread.interrupt()
      this.promise.success(())
    }
  }

  override def run(): Unit = {
    //
    this.running.set(Thread.currentThread())
    val result = body

    //
    synchronized {
      this.running.set(null)
      if (!this.promise.isCompleted) this.promise.complete(result)
    }
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
    Task { Try { while (true) { body } } }

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