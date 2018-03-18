package beaker.common

import scala.concurrent.Future
import scala.util.Try

package object util {

  /**
   *
   * @param f
   * @tparam T
   * @return
   */
  def ensure[T](f: => Future[T]): Future[T] = f recoverWith { case _ => ensure(f) }

  /**
   *
   * @param f
   * @tparam T
   * @return
   */
  def ensure[T](f: => Try[T]): Try[T] = f recoverWith { case _ => ensure(f) }

  /**
   *
   * @param f
   */
  def ensure(f: => Boolean): Unit = if (!f) ensure(f)

}
