package beaker.common.util

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * A retry scheduler.
 */
trait Retry {

  /**
   * Retries until the specified task completes successfully.
   *
   * @param f Task.
   * @param ec Implicit execution context.
   * @return Successful result.
   */
  def ensure[T](f: => Future[T])(implicit ec: ExecutionContext): Future[T] =
    f recoverWith { case _ => ensure(f) }

  /**
   * Retries until the specified task completes successfully.
   *
   * @param f Task.
   * @return Successful result.
   */
  def ensure[T](f: => Try[T]): Try[T] =
    f recoverWith { case _ => ensure(f) }

  /**
   * Retries until the specified condition is true.
   *
   * @param f Condition.
   */
  def ensure(f: => Boolean): Unit =
    if (!f) ensure(f)

}