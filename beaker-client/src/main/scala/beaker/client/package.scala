package beaker

import beaker.server.protobuf.Address

package object client {

  type Key = String
  type Version = Long
  type Value = String

  // Implicit Operations.
  implicit class ToAddress(x: String) {

    /**
     * Converts the string to a network address.
     *
     * @return Address.
     */
    def toAddress: Address = Address(x.split(":").head, x.split(":")(1).toInt)

  }

}
