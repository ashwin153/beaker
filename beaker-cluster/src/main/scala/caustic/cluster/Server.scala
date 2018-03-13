package caustic.cluster

import java.io.Closeable

/**
 * A remote instance.
 */
trait Server extends Closeable {

  sys.addShutdownHook(this.close())

  /**
   * Returns the [[Address]] at which this [[Server]] is accessible.
   *
   * @return
   */
  def address: Address

  /**
   * Makes this [[Server]] accessible.
   */
  def serve(): Unit

}