package caustic.cluster

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AddressTest extends FunSuite with Matchers {

  test("Serialization is reversible.") {
    val address = Address.local(9090)
    address shouldEqual Address(address.toBytes)
  }

}
