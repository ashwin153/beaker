package caustic.cluster

import java.net.InetAddress
import java.nio.charset.Charset

/**
 * A serializable network location.
 *
 * @param host Hostname.
 * @param port Port number.
 */
case class Address(host: String, port: Int) {

  /**
   * Returns the serialized representation.
   *
   * @return Serialized bytes.
   */
  def toBytes: Array[Byte] = s"$host:$port".getBytes(Address.Repr)

}

object Address {

  // Default character representation.
  val Repr: Charset = Charset.forName("UTF-8")

  /**
   * Constructs an [[Address]] for the specified port on the local machine.
   *
   * @param port Port number.
   * @return Localhost [[Address]].
   */
  def local(port: Int): Address = {
    Address(InetAddress.getLocalHost.getHostAddress, port)
  }

  /**
   * Constructs an [[Address]] from the serialized representation.
   *
   * @param bytes Serialized representation.
   * @return Deserialized [[Address]].
   */
  def apply(bytes: Array[Byte]): Address = {
    val tokens = new String(bytes, Address.Repr).split(":")
    Address(tokens(0), tokens(1).toInt)
  }

}