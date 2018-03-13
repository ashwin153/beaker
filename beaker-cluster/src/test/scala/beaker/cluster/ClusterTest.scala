package beaker.cluster

import beaker.cluster.Cluster.Dynamic
import org.apache.curator.test.TestingServer
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}

@RunWith(classOf[JUnitRunner])
class ClusterTest extends FunSuite with MockitoSugar with Eventually {

  implicit val defaultPatience: PatienceConfig = PatienceConfig(
    timeout = Span(15, Seconds),
    interval = Span(100, Millis)
  )

  test("Clusters are dynamically updated") {
    // Construct registries backed by an in-memory ZooKeeper.
    val zookeeper = new TestingServer(true)
    val x = Dynamic(mock[Service[_]], Dynamic.Config(zookeeper.getConnectString))
    val y = Dynamic(mock[Service[_]], Dynamic.Config(zookeeper.getConnectString))

    // Verify that registration events are propagated between them.
    val address = Address.local(9090)
    x.join(address)
    eventually(y.members.contains(address))
    x.leave(address)
    eventually(!y.members.contains(address))

    // Close the registries and the ZooKeeper instance.
    x.close()
    y.close()
    zookeeper.close()
  }

}
